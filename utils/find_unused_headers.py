#!/usr/bin/env python3
"""
Find unused headers in ClickHouse source files.

This script uses clang-include-cleaner to identify potentially unused headers,
then verifies each removal by attempting to compile the file. Only headers
that can be safely removed (compilation still succeeds) are reported.

This is similar to how CLion detects unused headers - it only flags headers
that are truly not needed for successful compilation. But not quite so - some headers
may alter the behavior of the code that still compiles without them.

That's why this tool can't guarantee 100% correctness.

Usage:
    ./find_unused_headers.py <source_file.cpp> [--build-dir BUILD_DIR] [--apply]
    ./find_unused_headers.py --batch <directory> [--build-dir BUILD_DIR] [--jobs N]
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import shutil
from pathlib import Path
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_compile_command(source_file: str, build_dir: str) -> Optional[dict]:
    """Extract compile command for the given source file from compile_commands.json."""
    compile_commands_path = os.path.join(build_dir, "compile_commands.json")
    if not os.path.exists(compile_commands_path):
        print(f"Error: compile_commands.json not found in {build_dir}", file=sys.stderr)
        return None

    with open(compile_commands_path, 'r') as f:
        commands = json.load(f)

    source_file_abs = os.path.abspath(source_file)
    for entry in commands:
        if os.path.abspath(entry.get('file', '')) == source_file_abs:
            return entry

    # Try matching by basename if absolute path doesn't work
    source_basename = os.path.basename(source_file)
    for entry in commands:
        if os.path.basename(entry.get('file', '')) == source_basename:
            if source_file in entry.get('file', ''):
                return entry

    return None


def get_unused_includes(source_file: str, build_dir: str) -> List[Tuple[str, int]]:
    """Use clang-include-cleaner to get list of potentially unused includes."""
    try:
        result = subprocess.run(
            ['clang-include-cleaner', '-p', build_dir, '--print=changes', '--remove', source_file],
            capture_output=True,
            text=True,
            timeout=120
        )
    except subprocess.TimeoutExpired:
        print(f"Timeout analyzing {source_file}", file=sys.stderr)
        return []
    except FileNotFoundError:
        print("Error: clang-include-cleaner not found", file=sys.stderr)
        return []

    unused = []
    for line in result.stdout.split('\n'):
        # Parse lines like: - <IO/ReadBuffer.h> @Line:13
        match = re.match(r'^- [<"]([^>"]+)[>"] @Line:(\d+)', line)
        if match:
            header = match.group(1)
            line_num = int(match.group(2))
            unused.append((header, line_num))

    return unused


def test_compilation(source_file: str, compile_cmd: dict) -> bool:
    """Test if the source file compiles successfully."""
    command = compile_cmd.get('command') or ' '.join(compile_cmd.get('arguments', []))
    directory = compile_cmd.get('directory', '.')

    # Modify command to only check syntax (not generate object file)
    # Replace -o <output> with -fsyntax-only
    command = re.sub(r'-o\s+\S+', '-fsyntax-only', command)

    # Remove module-related flags that cause issues with temp files
    # These flags reference .modmap files that won't exist for our temp files
    command = re.sub(r'@\S+\.modmap', '', command)
    command = re.sub(r'-fmodules\S*', '', command)

    # Remove -c flag as it conflicts with -fsyntax-only
    command = re.sub(r'\s-c\s', ' ', command)

    # Replace the source file in command with our test file
    original_file = compile_cmd.get('file', '')
    if original_file:
        command = command.replace(original_file, source_file)

    try:
        result = subprocess.run(
            command,
            shell=True,
            cwd=directory,
            capture_output=True,
            text=True,
            timeout=60
        )
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        return False
    except Exception as e:
        print(f"Compilation test failed with exception: {e}", file=sys.stderr)
        return False


def remove_include_line(source_content: str, line_num: int) -> str:
    """Remove the include line at the specified line number."""
    lines = source_content.split('\n')
    if 1 <= line_num <= len(lines):
        # Check if it's actually an include line
        if '#include' in lines[line_num - 1]:
            lines[line_num - 1] = ''
    return '\n'.join(lines)


def find_quick_removals(source_file: str, build_dir: str, verbose: bool = False) -> List[Tuple[str, int]]:
    """Quick mode: just return clang-include-cleaner suggestions without verification."""
    candidates = get_unused_includes(source_file, build_dir)
    if verbose and candidates:
        print(f"Found {len(candidates)} candidate(s) in {source_file}")
        for header, line_num in candidates:
            print(f"  {header} (line {line_num})")
    return candidates


def find_safe_removals(source_file: str, build_dir: str, verbose: bool = False) -> List[Tuple[str, int]]:
    """Find headers that can be safely removed from the source file."""
    # Get compile command
    compile_cmd = get_compile_command(source_file, build_dir)
    if not compile_cmd:
        if verbose:
            print(f"No compile command found for {source_file}", file=sys.stderr)
        return []

    # Get list of potentially unused includes
    candidates = get_unused_includes(source_file, build_dir)
    if not candidates:
        if verbose:
            print(f"No unused include candidates found in {source_file}")
        return []

    if verbose:
        print(f"Found {len(candidates)} candidate(s) to check in {source_file}")

    # Read original source
    with open(source_file, 'r') as f:
        original_content = f.read()

    safe_removals = []

    # Test each removal individually
    for header, line_num in candidates:
        # Create modified content with this include removed
        modified_content = remove_include_line(original_content, line_num)

        # Write to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.cpp', delete=False) as tmp:
            tmp.write(modified_content)
            tmp_path = tmp.name

        try:
            # Test if it still compiles
            if test_compilation(tmp_path, compile_cmd):
                safe_removals.append((header, line_num))
                if verbose:
                    print(f"  SAFE: {header} (line {line_num})")
            else:
                if verbose:
                    print(f"  NEEDED: {header} (line {line_num})")
        finally:
            os.unlink(tmp_path)

    return safe_removals


def apply_removals(source_file: str, removals: List[Tuple[str, int]]) -> None:
    """Apply the removals to the source file."""
    with open(source_file, 'r') as f:
        lines = f.read().split('\n')

    # Sort by line number in reverse order to maintain correct line numbers while removing
    removals_sorted = sorted(removals, key=lambda x: x[1], reverse=True)

    for header, line_num in removals_sorted:
        if 1 <= line_num <= len(lines) and '#include' in lines[line_num - 1]:
            # Remove the line
            del lines[line_num - 1]

    with open(source_file, 'w') as f:
        f.write('\n'.join(lines))


def process_file(args: Tuple[str, str, bool, bool]) -> Tuple[str, List[Tuple[str, int]]]:
    """Process a single file (for parallel execution)."""
    source_file, build_dir, verbose, quick = args
    if quick:
        removals = find_quick_removals(source_file, build_dir, verbose)
    else:
        removals = find_safe_removals(source_file, build_dir, verbose)
    return (source_file, removals)


def find_source_files(directory: str) -> List[str]:
    """Find all C++ source files in the directory."""
    extensions = ['.cpp', '.cc', '.cxx', '.c']
    source_files = []
    for ext in extensions:
        source_files.extend(Path(directory).rglob(f'*{ext}'))
    return [str(f) for f in source_files]


def main():
    parser = argparse.ArgumentParser(
        description='Find unused headers in ClickHouse source files'
    )
    parser.add_argument('source', help='Source file or directory to analyze')
    parser.add_argument('--build-dir', '-p', default='build',
                        help='Path to build directory containing compile_commands.json')
    parser.add_argument('--apply', action='store_true',
                        help='Apply the removals to the source file(s)')
    parser.add_argument('--batch', action='store_true',
                        help='Process all source files in the given directory')
    parser.add_argument('--jobs', '-j', type=int, default=1,
                        help='Number of parallel jobs for batch processing')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Verbose output')
    parser.add_argument('--output', '-o', help='Output results to a file')
    parser.add_argument('--quick', '-q', action='store_true',
                        help='Quick mode: only use clang-include-cleaner without compilation verification')

    args = parser.parse_args()

    results = []

    # Select the appropriate function based on mode
    find_func = find_quick_removals if args.quick else find_safe_removals

    if args.batch or os.path.isdir(args.source):
        # Process all files in directory
        source_files = find_source_files(args.source)
        print(f"Found {len(source_files)} source file(s) to analyze")
        if args.quick:
            print("Running in QUICK mode (no compilation verification)")

        if args.jobs > 1:
            with ThreadPoolExecutor(max_workers=args.jobs) as executor:
                futures = [
                    executor.submit(process_file, (f, args.build_dir, args.verbose, args.quick))
                    for f in source_files
                ]
                for future in as_completed(futures):
                    source_file, removals = future.result()
                    if removals:
                        results.append((source_file, removals))
                        if not args.verbose:
                            print(f"{source_file}: {len(removals)} unused header(s)")
        else:
            for source_file in source_files:
                removals = find_func(source_file, args.build_dir, args.verbose)
                if removals:
                    results.append((source_file, removals))
                    if not args.verbose:
                        print(f"{source_file}: {len(removals)} unused header(s)")
    else:
        # Process single file
        removals = find_func(args.source, args.build_dir, verbose=True)
        if removals:
            results.append((args.source, removals))

    # Output results
    if results:
        print("\n" + "=" * 60)
        print("UNUSED HEADERS THAT CAN BE SAFELY REMOVED:")
        print("=" * 60)
        for source_file, removals in results:
            print(f"\n{source_file}:")
            for header, line_num in removals:
                print(f"  Line {line_num}: #include <{header}>")

        if args.output:
            with open(args.output, 'w') as f:
                for source_file, removals in results:
                    for header, line_num in removals:
                        f.write(f"{source_file}:{line_num}:{header}\n")
            print(f"\nResults written to {args.output}")

        if args.apply:
            print("\nApplying removals...")
            for source_file, removals in results:
                apply_removals(source_file, removals)
                print(f"  Updated {source_file}")
    else:
        print("No unused headers found that can be safely removed.")


if __name__ == '__main__':
    main()
