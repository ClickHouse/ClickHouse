#!/usr/bin/env python3
"""
Rebuild jeprof reports for all queries in a results.json file.

This script regenerates jeprof reports (text, objects, collapsed stacks, 
call graph SVG, flame graph SVG) from existing heap profile files.

Useful when:
- Previous run timed out during report generation
- You want to regenerate reports with different options
- Some reports are missing or corrupted

Usage:
    python3 rebuild_reports.py <output_directory>
    
Example:
    python3 rebuild_reports.py profiler_output
"""
import json
import subprocess
import os
import sys
import glob
import argparse

# Timeout for jeprof commands (seconds)
TIMEOUT = 120

def get_paths(output_dir):
    """Get paths relative to the output directory."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    clickhouse_root = os.path.join(script_dir, "../..")
    
    # The profiler binary is built from src/Parsers/examples/parser_memory_profiler.cpp
    return {
        'script_dir': script_dir,
        'build_dir': os.path.join(clickhouse_root, "build"),
        'profiler': os.path.join(clickhouse_root, "build/src/Parsers/examples/parser_memory_profiler"),
        'jeprof': os.path.expanduser("~/github/jemalloc/bin/jeprof"),
        'flamegraph': os.path.expanduser("~/FlameGraph/flamegraph.pl"),
        'output_dir': output_dir,
        'profiles_dir': os.path.join(output_dir, "profiles"),
        'results_file': os.path.join(output_dir, "results.json"),
    }


def run_jeprof(jeprof_path, args, timeout=TIMEOUT):
    """Run jeprof and return output, filtering 'Using local file' lines."""
    try:
        result = subprocess.run(
            [jeprof_path] + args,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        output = result.stdout
        # Filter out "Using local file" lines
        lines = [l for l in output.split('\n') if not l.startswith('Using local file')]
        return '\n'.join(lines)
    except subprocess.TimeoutExpired:
        print(f"    Timeout after {timeout}s")
        return ""
    except Exception as e:
        print(f"    Error: {e}")
        return ""


def main():
    parser = argparse.ArgumentParser(
        description='Rebuild jeprof reports from existing heap profiles',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 rebuild_reports.py profiler_output
    python3 rebuild_reports.py profiler_output_v2
        """
    )
    parser.add_argument('output_dir', help='Output directory containing profiles/ and results.json')
    parser.add_argument('--profiler', help='Path to parser_memory_profiler binary')
    parser.add_argument('--jeprof', help='Path to jeprof binary')
    parser.add_argument('--flamegraph', help='Path to flamegraph.pl')
    parser.add_argument('--timeout', type=int, default=TIMEOUT, help=f'Timeout for jeprof commands (default: {TIMEOUT}s)')
    
    args = parser.parse_args()
    
    paths = get_paths(args.output_dir)
    
    # Override paths if provided
    if args.profiler:
        paths['profiler'] = args.profiler
    if args.jeprof:
        paths['jeprof'] = args.jeprof
    if args.flamegraph:
        paths['flamegraph'] = args.flamegraph
    
    timeout = args.timeout
    
    # Check prerequisites
    if not os.path.exists(paths['results_file']):
        print(f"Error: results.json not found at {paths['results_file']}")
        sys.exit(1)
    
    if not os.path.exists(paths['profiler']):
        print(f"Error: profiler not found at {paths['profiler']}")
        print("Build it with: cd build && ninja parser_memory_profiler")
        sys.exit(1)
    
    if not os.path.exists(paths['jeprof']):
        print(f"Warning: jeprof not found at {paths['jeprof']}")
        print("Some reports may not be regenerated")
    
    # Load existing results
    with open(paths['results_file'], 'r') as f:
        results = json.load(f)
    
    print(f"Processing {len(results['queries'])} queries with {timeout}s timeout...")
    
    for query in results['queries']:
        idx = query['id']
        print(f"Query {idx}: {query['query'][:50]}...")
        
        # Find profile files
        before_files = sorted(glob.glob(os.path.join(paths['profiles_dir'], f"query_{idx}_before.*.heap")))
        after_files = sorted(glob.glob(os.path.join(paths['profiles_dir'], f"query_{idx}_after.*.heap")))
        
        if not before_files or not after_files:
            print(f"  No profile files found")
            continue
        
        before_file = before_files[-1]
        after_file = after_files[-1]
        
        # Text report (bytes)
        if not query.get('jeprof_text') or query['jeprof_text'].strip() == '':
            print(f"  Generating text report...")
            query['jeprof_text'] = run_jeprof(paths['jeprof'], [
                "--text", "--show_bytes",
                f"--base={before_file}",
                paths['profiler'], after_file
            ], timeout)
        
        # Objects report
        if not query.get('jeprof_objects') or query['jeprof_objects'].strip() == '':
            print(f"  Generating objects report...")
            query['jeprof_objects'] = run_jeprof(paths['jeprof'], [
                "--text", "--inuse_objects",
                f"--base={before_file}",
                paths['profiler'], after_file
            ], timeout)
        
        # Call graph SVG
        if not query.get('graph_svg') or query['graph_svg'].strip() == '' or '<svg' not in query.get('graph_svg', ''):
            print(f"  Generating call graph SVG...")
            svg = run_jeprof(paths['jeprof'], [
                "--svg", "--show_bytes",
                "--nodefraction=0", "--edgefraction=0",
                f"--base={before_file}",
                paths['profiler'], after_file
            ], timeout)
            # Extract just the SVG part and clean up header text
            if '<svg' in svg:
                import re
                svg_start = svg.find('<svg')
                svg = svg[svg_start:]
                # Remove header text elements
                svg = re.sub(r'<text[^>]*>\s*/Users/[^<]*</text>', '', svg)
                svg = re.sub(r'<text[^>]*>\s*\./[^<]*</text>', '', svg)
                svg = re.sub(r'<text[^>]*>\s*Total B:[^<]*</text>', '', svg)
                svg = re.sub(r'<text[^>]*>\s*Focusing on:[^<]*</text>', '', svg)
                svg = re.sub(r'<text[^>]*>\s*Dropped nodes[^<]*</text>', '', svg)
                svg = re.sub(r'<text[^>]*>\s*Dropped edges[^<]*</text>', '', svg)
                query['graph_svg'] = svg
            else:
                query['graph_svg'] = svg
        
        # Collapsed stacks for flame graph
        if not query.get('collapsed_stacks') or query['collapsed_stacks'].strip() == '':
            print(f"  Generating collapsed stacks...")
            query['collapsed_stacks'] = run_jeprof(paths['jeprof'], [
                "--collapsed", "--show_bytes",
                f"--base={before_file}",
                paths['profiler'], after_file
            ], timeout)
        
        # Flame graph SVG
        if not query.get('flame_svg') or query['flame_svg'].strip() == '':
            if query.get('collapsed_stacks') and query['collapsed_stacks'].strip():
                if os.path.exists(paths['flamegraph']):
                    print(f"  Generating flame graph...")
                    try:
                        result = subprocess.run(
                            [paths['flamegraph'], "--title", f"Query {idx} Memory", "--countname", "bytes", "--colors", "mem"],
                            input=query['collapsed_stacks'],
                            capture_output=True,
                            text=True,
                            timeout=30
                        )
                        query['flame_svg'] = result.stdout
                    except Exception as e:
                        print(f"    Flame graph error: {e}")
        
        # Save after each query in case of interruption
        with open(paths['results_file'], 'w') as f:
            json.dump(results, f, indent=2)
    
    print("\nDone!")
    print(f"Results saved to: {paths['results_file']}")
    print(f"\nTo generate HTML report:")
    print(f"  python3 generate_report.py -i {paths['results_file']} -o {paths['output_dir']}/report.html")


if __name__ == '__main__':
    main()
