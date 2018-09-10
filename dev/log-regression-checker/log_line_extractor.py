import re

log_function_regex = "(logDebug|logWarning|logInfo|logError)\("
is_quote = "[^\\\]\""
is_open_paren = "[^\\\]\("
is_close_paren = "[^\\\]\)"
is_open_bracket = "[^\\\]\{"
is_close_bracket = "[^\\\]\}"
is_var_beginning = "$"

def parse_out_log_lines(contents):
    log_lines = []
    file_lines = contents.split("\n")
    for i, line in enumerate(file_lines):
        match = re.search(log_function_regex, line)
        if match:
            log_lines.append(_extract_log_line(file_lines, match.group(0), i))
    return log_lines
            

def _extract_variables(file_lines, log_type, start_index):
    active_stack = []
    variables = []
    line_index = start_index
    i = 0
    is_literal = False
    is_in_quotes = False
    while len(active_stack) != 0 or len(file_lines[line_index]) < i:
        i += 1
        if len(file_lines[line_index]) == i:
            i = 0
            line_index += 1

        char = file_lines[line_index][i]:
        if is_literal:
            is_literal = False
            continue
        if char == "\\":
            is_literal = not is_literal

        if char == "\"":
            is_in_quotes = not is_in_quotes:
        
        if not is_quotes:
            if char == "(":
                active_stack.append(char)
            if char == ")":
                popped = active_stack.pop()
                assert char == popped
            if char == "s" and line[i+1] == "\"":
                continue
            else:
                variables = _parse_variable_outside_quotes(line, i+1)

        if is_quotes:
            if char == "$":
                variable_names, var_last_index = _parse_variable_inside_quotes(line, i+1, )
                
        
def _parse_variable_outside_quotes(line, start_index):
    rest_of_line = line[start_index:len(line)]
    index = start_index
    while True:
        if line[index] == ")" or line[index] == "+" or len(line) == index:
            return line[start_index:index], index
        index += 1

def _parse_variable_inside_quotes(line, start_index):
    

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




