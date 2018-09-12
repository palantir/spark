import unittest
import os
from log_regression_checker import *
from log_line_extractor import *

current_file_dir = os.path.dirname(os.path.realpath(__file__))

class TestStringMethods(unittest.TestCase):
    def test_basic(self):
        expected_lines = [
            ["variable"],
            ["variable"],
            ["variable"],
            ["variable"],
            [],
            ["variable"],
            ["variable"]
        ]
        with open(os.path.join(current_file_dir, "test-files/BasicTests.scala")) as f:
            content = f.read()
        log_lines = parse_out_log_lines(content)
        assert len(log_lines) == len(expected_lines)
        for i in range(0, len(log_lines)):
            assert log_lines[i] == expected_lines[i]
    
    def test_variable_in_quotes(self):
        expected_lines = [
            ["variable"],
            ["variable"],
            ["variable"],
            ["variable"],
            ["variable"],
            ["variable"],
            ["variable"],
            ["variable"],
            ["var1", "var2", "var3", "var4"],
            ["var1", "var2"]
        ]
        with open(os.path.join(current_file_dir, "test-files/VariableInQuotes.scala")) as f:
            content = f.read()
        log_lines = parse_out_log_lines(content)
        assert len(log_lines) == len(expected_lines)
        for i in range(0, len(log_lines)):
            print i, log_lines[i], expected_lines[i]
            assert log_lines[i] == expected_lines[i]
    

if __name__ == '__main__':
    unittest.main()
    
