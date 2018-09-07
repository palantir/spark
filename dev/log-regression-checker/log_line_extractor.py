import re

log_function_regex = "(logDebug\(|logWarning\(|logInfo\(|logError\()"

def parse_out_log_lines(contents):
    log_lines = set()
    file_lines = contents.split("\n")
    for i, line in enumerate(file_lines):
        if re.search(log_function_regex, line):
            log_lines.add(extract_log_line(file_lines, i))
    return log_lines
            

def extract_log_line(file_lines, start_index):
    log_line_string = file_lines[start_index].strip()
    j = start_index + 1
    while True:
        line = file_lines[j].strip()
        if line.startswith("s\"") or line.startswith("\"") or line.startswith("+"):
            log_line_string += line
            j += 1
        else:
            break
    return log_line_string


