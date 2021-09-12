#!/usr/bin/python3

import os
import time

TEST_FILE_EXTENSIONS = ['.sql', '.sql.j2', '.sh', '.py', '.expect']
TEST_FILE_EXTENSIONS += [ext + '.disabled' for ext in TEST_FILE_EXTENSIONS]
TAGS_PREFIX = "Tags:"


def parse_skip_list_json(text):
    pos = 0
    def skip_spaces():
        nonlocal pos
        start_pos = pos
        while pos < len(text) and text[pos].isspace():
            pos += 1
        return pos > start_pos

    def skip_comment():
        nonlocal pos
        if text.startswith('//', pos):
            end_of_line = text.find('\n', pos + 1)
            if end_of_line == -1:
                pos = len(text)
                return True
            pos = end_of_line + 1
            return True
        elif text.startswith('/*', pos):
            end_of_comment = text.find('*/', pos + 2)
            if end_of_comment == -1:
                raise Exception(f'Not found the end of a comment at pos {pos}')
            pos = end_of_comment + 2
            return True
        else:
            return False

    def skip_spaces_or_comments():
        while skip_spaces() or skip_comment():
            pass

    def skip_char_only(c):
        nonlocal pos
        if not text.startswith(c, pos):
            return False
        pos += 1
        return True

    def skip_char(c):
        skip_spaces_or_comments()
        return skip_char_only(c)

    def expect_char(c):
        nonlocal pos
        skip_spaces_or_comments()
        if not text.startswith(c, pos):
            raise Exception(f"Expected '{c}' at pos {pos}")
        pos += 1

    def parse_quoted_string():
        nonlocal pos
        skip_spaces_or_comments()
        expect_char('"')
        end_of_string = text.find('"', pos)
        if end_of_string == -1:
            raise Exception(f'Not found the end of a quoted string at pos {pos-1}')
        str = text[pos:end_of_string]
        pos = end_of_string + 1
        return str

    def parse_description_comment():
        nonlocal pos
        while pos < len(text) and (text[pos] == ' ' or text[pos] == '\t'):
            pos += 1
        if not text.startswith('///', pos):
            return None
        end_of_line = text.find('\n', pos + 3)
        if end_of_line == -1:
            description = text[pos+3:]
            pos = len(text)
        else:
            description = text[pos+3:end_of_line]
            pos = end_of_line + 1
        description = description.strip()
        return description

    res = {}
    expect_char('{')
    while not skip_char('}'):
        build_flag = parse_quoted_string()
        expect_char(':')
        expect_char('[')
        patterns = []
        while not skip_char(']'):
            pattern = parse_quoted_string()
            skip_char_only(',')
            description = parse_description_comment()
            patterns.append((pattern, description))
        skip_char(',')
        res[build_flag] = patterns
    return res


def load_skip_list_json(path):
    if not os.path.exists(path):
        raise Exception(f'File {path} not found')
    with open(path, 'r') as file:
        contents = file.read()
        skip_dict = parse_skip_list_json(contents)
        return skip_dict


def parse_fasttest_run_sh(text):
    mark = 'TESTS_TO_SKIP=('
    pos = text.find(mark)
    if pos == -1:
        raise Exception('TESTS_TO_SKIP not found in fasttest/run.sh')
    pos += len(mark)

    def skip_spaces():
        nonlocal pos
        start_pos = pos
        while pos < len(text) and text[pos].isspace():
            pos += 1
        return pos > start_pos

    def skip_comment():
        nonlocal pos
        if text.startswith('#', pos):
            end_of_line = text.find('\n', pos + 1)
            if end_of_line == -1:
                pos = len(text)
                return True
            pos = end_of_line + 1
            return True
        else:
            return False

    def skip_spaces_or_comments():
        while skip_spaces() or skip_comment():
            pass

    def skip_char(c):
        nonlocal pos
        skip_spaces_or_comments()
        if not text.startswith(c, pos):
            return False
        pos += 1
        return True

    def parse_test_pattern():
        nonlocal pos
        skip_spaces_or_comments()
        cur_pos = pos
        while (cur_pos < len(text)) and (text[cur_pos].isalnum() or text[cur_pos] == '_'):
            cur_pos += 1
        if cur_pos == pos:
            raise Exception(f"Couldn't read a test's name or pattern at pos {pos}")
        pattern = text[pos:cur_pos]
        pos = cur_pos
        return pattern
        
    def parse_description_comment():
        nonlocal pos
        while pos < len(text) and (text[pos] == ' ' or text[pos] == '\t'):
            pos += 1
        if not text.startswith('#', pos):
            return None
        end_of_line = text.find('\n', pos + 1)
        if end_of_line == -1:
            description = text[pos+1:]
            pos = len(text)
        else:
            description = text[pos+1:end_of_line]
            pos = end_of_line + 1
        description = description.strip()
        return description

    patterns = []
    while not skip_char(')'):
        pattern = parse_test_pattern()
        description = parse_description_comment()
        patterns.append((pattern, description))
    return {"fasttest": patterns}


def load_fasttest_run_sh(path):
    if not os.path.exists(path):
        raise Exception(f'File {path} not found')
    with open(path, 'r') as file:
        contents = file.read()
        skip_dict = parse_fasttest_run_sh(contents)
        return skip_dict


def get_comment_sign(filename):
    if filename.endswith('.disabled'):
        filename = filename[:-len('.disabled')]
    if filename.endswith('.sql') or filename.endswith('.sql.j2'):
        return '--'
    elif filename.endswith('.sh') or filename.endswith('.py') or filename.endswith('.expect'):
        return '#'
    else:
        raise Exception(f'Unknown file_extension: {filename}')


def is_shebang(line):
    return line.startswith('#!')


def parse_tags_from_line(line, comment_sign):
    if not line.startswith(comment_sign):
        return None
    tags_str = line[len(comment_sign):].lstrip()
    if not tags_str.startswith(TAGS_PREFIX):
        return None
    tags_str = tags_str[len(TAGS_PREFIX):]
    tags = tags_str.split(',')
    tags = [tag.strip() for tag in tags]
    return tags


def format_tags_to_line(tags, comment_sign):
    return comment_sign + ' ' + TAGS_PREFIX + ' ' + ', '.join(tags) + '\n'


def load_tags_from_file(filepath):
    with open(filepath, 'r') as file:
        try:
            line = file.readline()
            if is_shebang(line):
                line = file.readline()
        except UnicodeDecodeError:
            return []
    return parse_tags_from_line(line, get_comment_sign(filepath))


def save_tags_to_file(filepath, tags, description = {}):
    if not tags:
        return
    with open(filepath, 'r') as file:
        contents = file.read()

    # Skip shebang.
    shebang_line = ''
    eol = contents.find('\n')
    if eol != -1 and is_shebang(contents[:eol+1]):
        shebang_line = contents[:eol+1]
        contents = contents[eol+1:]

    # Skip tag line with description lines.
    eol = contents.find('\n')
    comment_sign = get_comment_sign(filepath)
    if eol != -1 and parse_tags_from_line(contents[:eol+1], comment_sign):
        contents = contents[eol+1:]
        while True:
            eol = contents.find('\n')
            if eol == -1 or contents[:eol+1].isspace():
                break
            contents = contents[eol+1:]

    # Skip an empty line after tags.
    eol = contents.find('\n')
    if eol != -1 and contents[:eol+1].isspace():
        contents = contents[eol+1:]
    empty_line = '\n'

    # New tags line.
    tags_line = format_tags_to_line(tags, comment_sign)

    # New description lines.
    tags_with_descriptions = []
    for tag in tags:
        if description.get(tag):
            tags_with_descriptions.append(tag)
    
    description_lines = ''
    if tags_with_descriptions:
        for tag in tags_with_descriptions:
            description_lines += comment_sign + ' Tag ' + tag + ': ' + description[tag] + '\n'

    contents = shebang_line + tags_line + description_lines + empty_line + contents
    with open(filepath, 'w') as file:
        file.write(contents)
        print(f'Changed {filepath}')


def build_flag_to_tag(build_flag):
    if build_flag == "thread-sanitizer":
        return "no-tsan"
    elif build_flag == "address-sanitizer":
        return "no-asan"
    elif build_flag == "ub-sanitizer":
        return "no-ubsan"
    elif build_flag == "memory-sanitizer":
        return "no-msan"
    elif build_flag == "database-replicated":
        return "no-replicated-database"
    elif build_flag == "database-ordinary":
        return "no-ordinary-database"
    elif build_flag == "debug-build":
        return "no-debug"
    elif build_flag == "release-build":
        return "no-release"
    elif build_flag == "unbundled-build":
        return "no-unbundled"
    else:
        return "no-" + build_flag


def get_tags(test_name, skip_list):
    tags = []
    descriptions = {}

    if test_name.endswith('.disabled'):
        tags.append("disabled")

    if 'deadlock' in test_name:
        tags.append("deadlock")
    elif 'race' in test_name:
        tags.append("race")
    elif 'long' in test_name:
        tags.append("long")

    if 'replica' in test_name:
        tags.append("replica")
    elif 'zookeeper' in test_name:
        tags.append("zookeeper")

    if 'distributed' in test_name:
        tags.append("distributed")
    elif 'shard' in test_name:
        tags.append("shard")
    elif 'global' in test_name:
        tags.append("global")

    for build_flag, patterns in skip_list.items():
        for pattern, description in patterns:
            if pattern in test_name:
                tag = build_flag_to_tag(build_flag)
                if not tag in tags:
                    tags.append(tag)
                    if description:
                        descriptions[tag] = description

    return tags, descriptions


def load_all_tags(base_dir):
    def get_comment_sign(filename):
        if filename.endswith('.sql') or filename.endswith('.sql.j2'):
            return '--'
        elif filename.endswith('.sh') or filename.endswith('.py') or filename.endswith('.expect'):
            return '#'
        else:
            raise Exception(f'Unknown file_extension: {filename}')

    def parse_tags_from_line(line, comment_sign):
        if not line.startswith(comment_sign):
            return None
        tags_str = line[len(comment_sign):].lstrip()
        tags_prefix = "Tags:"
        if not tags_str.startswith(tags_prefix):
            return None
        tags_str = tags_str[len(tags_prefix):]
        tags = tags_str.split(',')
        tags = {tag.strip() for tag in tags}
        return tags

    def is_shebang(line):
        return line.startswith('#!')

    def load_tags_from_file(filepath):
        with open(filepath, 'r') as file:
            try:
                line = file.readline()
                if is_shebang(line):
                    line = file.readline()
            except UnicodeDecodeError:
                return []
        return parse_tags_from_line(line, get_comment_sign(filepath))

    all_tags = {}
    for suite in os.listdir(base_dir):
        suite_dir = os.path.join(base_dir, suite)
        if not os.path.isdir(suite_dir):
            continue
        
        for test_name in os.listdir(suite_dir):
            test_path = os.path.join(suite_dir, test_name)
            if not os.path.isfile(test_path) or all(not test_path.endswith(supported_ext) for supported_ext in TEST_FILE_EXTENSIONS):
                continue
            tags = load_tags_from_file(test_path)
            if tags:
                all_tags[test_path] = tags
    return all_tags


def load_all_tags_timing(base_dir):
    start_time = time.time()
    print(type(load_all_tags(base_dir)))
    print("--- %s seconds ---" % (time.time() - start_time))


def main():
    base_dir = os.path.dirname(__file__)
    print(f'base_dir={base_dir}')

    #return load_all_tags_timing(base_dir)
    
    skip_dict = load_skip_list_json(os.path.join(base_dir, 'skip_list.json'))
    skip_dict_from_fasttest_run_sh = load_fasttest_run_sh(os.path.join(base_dir, '../../docker/test/fasttest/run.sh'))
    skip_dict = {**skip_dict, **skip_dict_from_fasttest_run_sh}
    #print(f'skip_dict={skip_dict}')

    for suite in os.listdir(base_dir):
        suite_dir = os.path.join(base_dir, suite)
        if not os.path.isdir(suite_dir):
            continue
        
        for test_name in os.listdir(suite_dir):
            test_path = os.path.join(suite_dir, test_name)
            if not os.path.isfile(test_path) or all(not test_path.endswith(supported_ext) for supported_ext in TEST_FILE_EXTENSIONS):
                continue
            tags, descriptions = get_tags(test_path, skip_dict)
            save_tags_to_file(test_path, tags, descriptions)

if __name__ == '__main__':
    main()
