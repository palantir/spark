import unittest
import os
from log_regression_checker import *
from log_line_extractor import *

current_file_dir = os.path.dirname(os.path.realpath(__file__))

class TestStringMethods(unittest.TestCase):
    def test_basic(self):
        expected_lines = [
            "(s\"Blah $variable blah\")",
            "(s\"Blah $variable blah\")",
            "(s\"Blah $variable blah\")",
            "(s\"Blah $variable blah\")",
            "(s\"blah no variable\")",
            "(s\"blah $variable blah\")",
            "(s\"blah $variable blah\")",
            "(s\"log line $var\", e)"
        ]
        with open(os.path.join(current_file_dir, "test-files/BasicTests.scala")) as f:
            content = f.read()
        log_lines = parse_log_lines(content)
        assert len(log_lines) == len(expected_lines)
        for i in range(0, len(log_lines)):
            assert log_lines[i] == expected_lines[i]
    
    def test_variable_in_quotes(self):
        expected_lines = [
            "(s\"blah $variable blah\")",
            "(s\"blah $variable\")",
            "(s\"$variable blah\")",
            "(s\"$variable\")",
            "(s\"${variable}\")",
            "(s\"${{variable}}\")",
            "(s\"${(variable)}\")",
            "(s\"blah${variable}blah\")",
            "(s\"${var1 - var2 + (var3 / var4)}\")",
            "(s\"${var1 + \"abc\" + var2}\")",
            "(s\"${var1 + s\"abc$var2\"} not_a_var\")",
            "(\"$$ \) ${var} \{\")"
        ]
        with open(os.path.join(current_file_dir, "test-files/VariableInQuotes.scala")) as f:
            content = f.read()
        log_lines = parse_log_lines(content)
        assert len(log_lines) == len(expected_lines)
        for i in range(0, len(log_lines)):
            assert log_lines[i] == expected_lines[i]
   
    def test_variable_outside_quotes(self):
        expected_lines = [
            "(variable.field)",
            "(var1 + var2 + \"blah $var3\" + var4)",
            "(variable, e)",
            "(\"blah\", e)",
            "(method(param))"
        ]
        with open(os.path.join(current_file_dir, "test-files/VariableOutsideQuotes.scala")) as f:
            content = f.read()
        log_lines = parse_log_lines(content)
        assert len(log_lines) == len(expected_lines)
        for i in range(0, len(log_lines)):
            assert log_lines[i] == expected_lines[i]

    def test_multi_line(self):
        expected_lines = [
            "(s\"String string $var string\" +" +
                "s\"String string $var2 string\" +" + 
                "s\"String string\" +" +
                "s\"String string $var3\")",
            "(s\"\"\"Block quote ${var}" +
                "|${var2} string string" +
                "\"\"\")"
        ]
        with open(os.path.join(current_file_dir, "test-files/MultilineLogs.scala")) as f:
            content = f.read()
        log_lines = parse_log_lines(content)
        assert len(log_lines) == len(expected_lines)
        for i in range(0, len(log_lines)):
            assert log_lines[i] == expected_lines[i]

if __name__ == '__main__':
    unittest.main()
    
