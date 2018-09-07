import re

log_function_regex = "(logDebug|logWarning|logInfo|logError)"

def parse_out_log_lines(contents):
    log_lines = []
    file_lines = contents.split("\n")
    for i, line in enumerate(file_lines):
        match = re.search(log_function_regex, line)
        if match:
            log_lines.append(_extract_log_line(file_lines, match.group(0), i))
    return log_lines
            

def _extract_log_line(file_lines, log_type, start_index):
    log_line_string = "    " + file_lines[start_index].strip()
    if not log_line_string.endswith("+"):
        return log_line_string
    j = start_index + 1
    while True:
        line = file_lines[j].strip()
        log_line_string = log_line_string + "\n        " + line
        j += 1
        if not line.endswith("+"):
            break
    return log_line_string




