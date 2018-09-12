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

class FileParser:
    def __init__(self, contents):
        self.contents = contents.split("\n")
        self.line_index = -1
        self.char_index = -1

    def _get_next_non_empty_line_index(self):
        index = self.line_index + 1
        while index < len(self.contents) and len(self.contents[index]) == 0:
            index += 1
        return index

    def get_current_line(self):
        return self.contents[self.line_index]

    def get_next_line(self):
        self.char_index = 0
        self.line_index = self._get_next_non_empty_line_index()
        if len(self.contents) <= self.line_index:
            return None
        return self.contents[self.line_index]

    def peek_next_line(self):
        next_index = self._get_next_non_empty_line_index()
        if len(self.contents) <= next_index:
            return None
        return self.contents[next_index]
    
    def get_current_char(self):
        return self.contents[self.line_index][self.char_index]

    def peek_next_char(self):
        next_index = self.char_index + 1
        if len(self.contents[self.line_index]) == self.char_index:
            next_line = self.peek_next_line()
            if next_line == None:
                return None
            return next_line[0]
        return self.contents[self.line_index][next_index]

    def get_next_char(self):
        self.char_index += 1
        if len(self.contents[self.line_index]) == self.char_index:
            next_line = self.get_next_line()
            if next_line == None:
                return None
            return next_line[self.char_index]
        return self.contents[self.line_index][self.char_index]

def parse_out_log_lines(contents):
    log_lines = []
    file_parser = FileParser(contents)
    while True:
        line = file_parser.get_next_line()
        if line == None:
            break
        match = re.search(log_function_regex, line)
        if match:
            log_lines.append(_extract_variables(file_parser, match.group(0)))
    return log_lines

def _extract_variables(file_parser, log_type):
    # Find the start of the log line
    active_stack = []
    while True:
        char = file_parser.get_next_char()
        if char == "(":
            active_stack.append(")")
            break
    
    variables = []
    is_literal = False
    is_in_quotes = False
    while len(active_stack) != 0 and file_parser.get_next_char():
        char = file_parser.get_current_char()
        if is_literal:
            is_literal = False
            continue
        if char == "\\":
            is_literal = not is_literal
        elif char == "\"":
            is_in_quotes = not is_in_quotes
        elif not is_in_quotes:
            if char == "(":
                active_stack.append(close_char[char])
            elif char == ")":
                popped = active_stack.pop()
                assert char == popped
            elif char == "s" and file_parser.peek_next_char() == "\"":
                continue
            elif char == ",":
                continue
            else:
                variables.extend(_parse_variable_outside_quotes(file_parser, [")", " "]))
                close_char = file_parser.get_current_char()
                if (close_char == ")"):
                    popped = active_stack.pop()
                    assert close_char == popped
        elif is_in_quotes:
            if char == "$": 
                if file_parser.peek_next_char() == "{":
                    file_parser.get_next_char()
                    close_chars = ["}"]
                else:
                    close_chars = ["\"", " "]
                file_parser.get_next_char()
                variables.extend(_parse_variable_outside_quotes(file_parser, close_chars))
                close_char = file_parser.get_current_char()
                if (close_char == "\""):
                    is_in_quotes = not is_in_quotes
    return variables

    
                
def _parse_variable_outside_quotes(file_parser, close_chars):
    print "entering", close_chars, file_parser.get_current_char()
    variable_str = ""
    all_vars = []
    while True:
        char = file_parser.get_current_char()
        if char in close_chars:
            if len(variable_str) != 0:
                all_vars.append(variable_str.strip())
            return all_vars
        
        if char in close_char.keys():
            file_parser.get_next_char()
            nested_variables = _parse_variable_outside_quotes(file_parser, [close_char[char]])
            assert file_parser.get_current_char() == close_char[char]
            all_vars.extend(nested_variables)
        if char == "\"":
            _parse_variable_outside_quotes(file_parser, ["\""])

        else:
            if char not in ["+", "-", "/", "*", " "]:
                variable_str += char
            if char == " " and len(variable_str) != 0:
                all_vars.append(variable_str.strip())
                variable_str = ""
        file_parser.get_next_char()
