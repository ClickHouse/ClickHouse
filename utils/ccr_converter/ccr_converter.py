import sys
from typing import TextIO
import argparse

abs_path_to_src = ""
files = []
tests = []
tests_names = []

# Generate a .info file with accumulated report. Does not preserve per-test data.
def convert_to_slim_genhtml_report():
    pass

def read_report(f: TextIO):
    global abs_path_to_src
    global files
    global tests
    global tests_names

    abs_path_to_src = f.readline()

    for i in range(int(f.readline().split()[1])): # files
        rel_path, funcs_count, lines_count = f.readline().split()

        funcs = {}

        for j in range(int(funcs_count)):
            mangled_name, start_line, edge_index = f.readline().split()
            funcs[int(edge_index)] = mangled_name, int(start_line)

        lines = [int(f.readline()) for j in range(int(lines_count))]

        files.append([rel_path, funcs, lines])

    tests_sources = {}

    while True:
        token = f.readline()

        print(token)

        if token != "TEST" and not token.startswith("SOURCE"):
            if len(tests_sources) > 0:
                tests.append(tests_sources)

            break

        if token == "TEST":
            if len(tests_sources) > 0:
                tests.append(tests_sources)
                tests_sources = {}

            token = f.readline()

        source_id, funcs_count, lines_count, = map(int, token.split()[1:])

        funcs_hit = {}
        lines_hit = {}

        for i in range(funcs_count):
            edge_index, call_count = map(int, f.readline().split())
            funcs_hit[edge_index] = call_count

        for i in range(lines_count):
            line, call_count = map(int, f.readline().split())
            lines_hit[line] = call_count

        tests_sources[source_id] = funcs_hit, lines_hit

    tests_names = f.readlines()

    print("Read the report. {} tests, {} source files".format(len(tests), len(files)))

def main():
    parser = argparse.ArgumentParser(prog='CCR converter')
    parser.add_argument('ccr_report_file')
    parser.add_argument('--genhtml-slim-report', nargs='?')

    args = parser.parse_args()

    with open(args.ccr_report_file, "r") as f:
        read_report(f)

    if args.genhtml_slim_report is not None:
        convert_to_slim_genhtml_report(args.genhtml_slim_report)

main()
