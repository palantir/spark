import unittest
import os
from log_regression_checker import *
from log_line_extractor import *

current_file_dir = os.path.dirname(os.path.realpath(__file__))

class TestStringMethods(unittest.TestCase):
    def test_parse_ExecutorPodsLifecycleManager(self):
        expected_lines = []
        with open(os.path.join(current_file_dir, "test-files/TestLogs.scala")) as f:
            content = f.read()
        log_lines = parse_out_log_lines(content)
        print log_lines

if __name__ == '__main__':
    unittest.main()

