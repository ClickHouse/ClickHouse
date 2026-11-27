#!/usr/bin/env python3

# Forgive my ignorance. This AI-generated script is used instead of a proper js bundler.

import re
import os
import sys

def inline_js_imports(file_path, included_files=None):
    if included_files is None:
        included_files = set()

    # If the file is already included, return an empty string
    if file_path in included_files:
        return ''

    # Add the current file to the set of included files
    included_files.add(file_path)

    try:
        with open(file_path, 'r') as file:
            content = file.read()
    except FileNotFoundError:
        return f'// Error: File {file_path} not found\n'

    # Remove multiline comments (/* */) but be careful not to remove content within string literals
    def remove_multiline_comments(text):
        pattern = re.compile(r'/\*.*?\*/', re.DOTALL)
        result = []
        in_string = False
        current_quote = ''
        i = 0
        while i < len(text):
            if text[i] in ('"', "'"):
                if not in_string:
                    in_string = True
                    current_quote = text[i]
                elif in_string and text[i] == current_quote:
                    in_string = False
            if not in_string and text[i:i+2] == '/*':
                end = text.find('*/', i+2)
                if end == -1:
                    break
                i = end + 2
            else:
                result.append(text[i])
                i += 1
        return ''.join(result)

    content = remove_multiline_comments(content)

    # Remove single-line comments (//) but not within string literals
    def remove_single_line_comments(text):
        lines = text.splitlines()
        result = []
        for line in lines:
            in_string = False
            current_quote = ''
            i = 0
            while i < len(line):
                if line[i] in ('"', "'"):
                    if not in_string:
                        in_string = True
                        current_quote = line[i]
                    elif in_string and line[i] == current_quote:
                        in_string = False
                if not in_string and line[i:i+2] == '//':
                    line = line[:i]
                    break
                i += 1
            result.append(line)
        return '\n'.join(result)

    content = remove_single_line_comments(content)

    # Regular expression to find import statements
    import_regex = re.compile(r"import\s+(?:\{[^}]+\}|\*\s+as\s+\w+)\s+from\s+'(.*\.js)';")

    # Function to replace import statements with file content
    def replace_import(match):
        import_file_path = match.group(1)
        # Resolve the relative path of the imported file
        resolved_path = os.path.abspath(os.path.join(os.path.dirname(file_path), import_file_path))
        # If the resolved path is already in the included files, return an empty line to avoid infinite recursion
        if resolved_path in included_files:
            return '\n'
        # Recursively get the content of the imported file
        return inline_js_imports(resolved_path, included_files)

    # Replace all import statements with the actual file content
    inlined_content = re.sub(import_regex, replace_import, content)

    return inlined_content

# Example usage
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: pack.py input.html > packed.html")
        sys.exit(1)

    file_path = sys.argv[1]
    result = inline_js_imports(file_path)
    print(result)
