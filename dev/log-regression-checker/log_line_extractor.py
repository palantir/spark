import re

log_function_regex = "(logDebug|logWarning|logInfo|logError)\("
is_quote = "[^\\\]\""
is_open_paren = "[^\\\]\("
is_close_paren = "[^\\\]\)"
is_open_bracket = "[^\\\]\{"
is_close_bracket = "[^\\\]\}"
is_var_beginning = "$"

close_char = {
    "(": ")",
    "{": "}"
    }

class FileContents:
    def __init__(self, contents):
        self.contents = contents.split("\n")
        self.line_index = 0
        self.char_index = 0

    def get_current_line(self):
        return self.contents[self.line_index]

    def get_next_line(self):
        self.line_index += 1
        self.char_index = 0
        if len(self.contents) == self.line_index:
            return None
        return self.contents[self.line_index]

    def get_current_char(self):
        return self.contents[self.line_index][self.char_index]

    def get_next_char(self):
        self.char_index += 1
        if len(self.contents[self.line_index]) == self.char_index:
            next_line = self.get_next_line()
            if next_line == None
                return None
            return next_line[self.char_index]
        return self.contents[self.line_index][self.char_index]

def parse_out_log_lines(contents):
    file_contents = FileContents(contents)
    log_lines = []
    file_lines = contents.split("\n")
    for i, line in enumerate(file_lines):
        match = re.search(log_function_regex, line)
        if match:
            print "sdfajsdf matching"
            log_lines.append(_extract_variables(file_lines, match.group(0), i))
    print log_lines
    return log_lines

def _extract_variables(file_lines, log_type, start_index):
    print "entered _extract_variables", start_index
    active_stack = []
    extracted_variables = []
    line_index = start_index
    i = file_lines[start_index].find("(") - 1
    is_literal = False
    is_in_quotes = False
    while len(active_stack) != 0 or i+1 < len(file_lines[line_index]):
        i += 1
        if len(file_lines[line_index]) == i:
            i = 0
            line_index += 1
        char = file_lines[line_index][i]
        if is_literal:
            is_literal = False
            continue
        elif char == "\\":
            is_literal = not is_literal

        elif char == "\"":
            is_in_quotes = not is_in_quotes
        
        elif not is_in_quotes:
            if char == "(":
                active_stack.append(close_char[char])
            elif char == ")":
                popped = active_stack.pop()
                assert char == popped
            elif char == "s" and line[i+1] == "\"":
                continue
            else:
                variables, index = _parse_variable_outside_quotes(file_lines[line_index], i+1)
                extracted_variables.extend(variables)

        elif is_in_quotes:
            if char == "$":
                variable_names, var_last_index = _parse_variable_outside_quotes(file_lines[line_index], i+1)
                extracted_variables.extend(variable_names)
    return extracted_variables
                
def _parse_variable_outside_quotes(line, start_index, close_chars=[")", "\"", " "]):
    rest_of_line = line[start_index:len(line)]
    index = start_index
    extracted_vars = []
    while True:
        char = line[index]
        if char in close_char.keys():
            nested_variables, index = _parse_variable_outside_quotes(line, index+1, [close_char[char]])
            assert line[index] == close_char[char] or line[index] == "\""
            return nested_variables, index
        if line[index] in close_chars or line[index] == "\"" or len(line) == (index):
            extracted_vars.append(line[start_index:index].strip())
            return extracted_vars, index

        index += 1

