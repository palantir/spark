import unittest
import os
from log_regression_checker import *
from log_line_extractor import *

current_file_dir = os.path.dirname(os.path.realpath(__file__))

class TestStringMethods(unittest.TestCase):
    def test_basic(self):
        expected_lines = [
            ("(s\"Blah $variable blah\")", 2),
            ("(s\"Blah $variable blah\")", 3),
            ("(s\"Blah $variable blah\")", 4),
            ("(s\"Blah $variable blah\")", 5),
            ("(s\"blah no variable\")", 8),
            ("(s\"blah $variable blah\")", 14),
            ("(s\"blah $variable blah\")", 15),
            ("(s\"log line $var\", e)", 18)
        ]
        with open(os.path.join(current_file_dir, "test-files/BasicTests.scala")) as f:
            content = f.read()
        log_lines = parse_log_lines(content)
        assert len(log_lines) == len(expected_lines)
        for i in range(0, len(log_lines)):
            assert log_lines[i] == expected_lines[i]
    
    def test_variable_in_quotes(self):
        expected_lines = [
            ("(s\"blah $variable blah\")", 2),
            ("(s\"blah $variable\")", 4),
            ("(s\"$variable blah\")", 5),
            ("(s\"$variable\")", 6),
            ("(s\"${variable}\")", 9),
            ("(s\"${{variable}}\")", 10),
            ("(s\"${(variable)}\")", 11),
            ("(s\"blah${variable}blah\")", 12),
            ("(s\"${var1 - var2 + (var3 / var4)}\")", 15),
            ("(s\"${var1 + \"abc\" + var2}\")", 18), 
            ("(s\"${var1 + s\"abc$var2\"} not_a_var\")", 19),
            ("(\"$$ \) ${var} \{\")", 22)
        ]
        with open(os.path.join(current_file_dir, "test-files/VariableInQuotes.scala")) as f:
            content = f.read()
        log_lines = parse_log_lines(content)
        assert len(log_lines) == len(expected_lines)
        for i in range(0, len(log_lines)):
            assert log_lines[i] == expected_lines[i]
   
    def test_variable_outside_quotes(self):
        expected_lines = [
            ("(variable.field)", 1),
            ("(var1 + var2 + \"blah $var3\" + var4)", 2),
            ("(variable, e)", 5),
            ("(\"blah\", e)", 6),
            ("(method(param))", 9)
        ]
        with open(os.path.join(current_file_dir, "test-files/VariableOutsideQuotes.scala")) as f:
            content = f.read()
        log_lines = parse_log_lines(content)
        assert len(log_lines) == len(expected_lines)
        for i in range(0, len(log_lines)):
            assert log_lines[i] == expected_lines[i]

    def test_multi_line(self):
        expected_lines = [
            ("(s\"String string $var string\" +" +
                "s\"String string $var2 string\" +" + 
                "s\"String string\" +" +
                "s\"String string $var3\")", 1),
            ("(s\"\"\"Block quote ${var}" +
                "|${var2} string string" +
                "\"\"\")", 6)
        ]
        with open(os.path.join(current_file_dir, "test-files/MultilineLogs.scala")) as f:
            content = f.read()
        log_lines = parse_log_lines(content)
        assert len(log_lines) == len(expected_lines)
        for i in range(0, len(log_lines)):
            assert log_lines[i] == expected_lines[i]

if __name__ == '__main__':
    unittest.main()
    
