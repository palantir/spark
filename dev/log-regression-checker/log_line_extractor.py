import re

log_function_regex = "(logDebug|logWarning|logInfo|logError)\("

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
        # iterate to the first non-space character in the line
        while (self.contents[self.line_index][self.char_index] == " "):
            self.char_index += 1
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

def parse_log_lines(contents):
    log_lines = []
    file_parser = FileParser(contents)
    while file_parser.get_next_line() != None:
        line = file_parser.get_current_line()
        if line == None:
            break
        if re.search(log_function_regex, line):
            log_lines.append(_extract_variables(file_parser))
    return log_lines

def _extract_variables(file_parser):
    # Find the start of the log line
    while (file_parser.get_next_char() != "("):
        continue
    return _parse_variable_outside_quotes(file_parser, [")"], "(")
    
def _parse_variable_inside_quotes(file_parser, log_string):
    is_literal = False
    while file_parser.get_next_char():
        char = file_parser.get_current_char()
        log_string += char
        if is_literal:
            is_literal = not is_literal
            continue
        if char == "\\":
            is_literal = True
            continue
        elif char == "$": 
            if file_parser.peek_next_char() == "$": # skip string literals $$
                log_string += file_parser.get_next_char()
                continue
            if file_parser.peek_next_char() == "{":
                log_string += file_parser.get_next_char()
                close_chars = ["}"]
            else:
                close_chars = ["\"", " "]
            log_string = _parse_variable_outside_quotes(file_parser, close_chars, log_string)
            close_char = file_parser.get_current_char()
            if (close_char == "\""):
                return log_string
        elif char == "\"":
            return log_string
                
def _parse_variable_outside_quotes(file_parser, close_chars, log_string):
    while file_parser.get_next_char():
        char = file_parser.get_current_char()
        log_string += char
        if char in close_chars:
            return log_string
        elif char == "\"":
            log_string = _parse_variable_inside_quotes(file_parser, log_string)
            assert file_parser.get_current_char() == "\""
        elif char in close_char.keys():
            log_string = _parse_variable_outside_quotes(file_parser, [close_char[char]], log_string)
            assert file_parser.get_current_char() == close_char[char]
