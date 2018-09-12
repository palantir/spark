import unittest
import os
from log_regression_checker import *
from log_line_extractor import *

current_file_dir = os.path.dirname(os.path.realpath(__file__))

class TestStringMethods(unittest.TestCase):
    def test_parse_ExecutorPodsLifecycleManager(self):
        expected_variables = [
            ["execId", "state.pod.getMetadata.getName"],
            ["execId", "state.pod.getMetadata.getName"],
            ["execId", "state.pod.getMetadata.getName"],
            ["exitReasonMessage"],
            ["execIdsRemovedInThisRound.mkString"]
        ]
        with open(os.path.join(current_file_dir, "test-files/ExecutorPodsLifecycleManager.scala")) as f:
            content = f.read()
        actual_variables = parse_log_lines(content)
        assert len(actual_variables) == 5
        for i in range(0, len(actual_variables)):
            assert actual_variables[i] == expected_variables[i]

    def test_update_master(self):
        config = {
            "master": { "a": "a.scala", "b": "b.scala" },
            "unmerged": { "b": "b.scala", "c": "c.scala" }
        }
        merged = update_master(config)
        expected = {
            "master": { "a": "a.scala", "b": "b.scala", "c": "c.scala" },
            "unmerged": {}
        }
        assert merged == expected
        
    def test_update_release(self):
        configs = {
            "master": { "a": "a.scala" },
            "unmerged": { "b": "b.scala" }
            }
        archives = {
            "tag-1": { "c": "c.scala" }
        }
        updated = update_release(configs, archives, "tag-2")
        expected = {
            "tag-1": { "c": "c.scala" },
            "tag-2": { "a": "a.scala", "b": "b.scala" }
        }
        assert updated == expected

    def test_check(self):
        filename = "test-files/ExecutorPodsLifecycleManager.scala"
        def _get_master_content(logfile):
            with open(os.path.join(current_file_dir, logfile)) as f:
                return f.read()
        def _get_current_content(logfile):
            with open(os.path.join(current_file_dir, logfile + ".unmerged")) as f:
                return f.read()
        config = {
            "master": {
                "file": filename
            },
            "unmerged": {}
            }
        failures = check(config, _get_master_content, _get_current_content, True)

        expected_unmodified_variables = [
            ["execId", "state.pod.getMetadata.getName"],
            ["execId", "state.pod.getMetadata.getName"],
            ["execId", "state.pod.getMetadata.getName"],
            ["exitReasonMessage"],
            ["execIdsRemovedInThisRound.mkString"]
        ]
        expected_modified_variables = [
            ["execId", "state.pod.getMetadata.getName"],
            ["execId", "state.pod.getMetadata.getName"],
            ["execId", "state.pod.getMetadata.getName"],
            ["exitReasonMessage"],
            []
        ]
        assert (filename in failures)
        assert len(failures.keys()) == 1
        assert failures[filename]["expected"] == expected_unmodified_variables
        assert failures[filename]["actual"] == expected_modified_variables

    def test_check_no_diff(self):
        filename = "test-files/ExecutorPodsLifecycleManager.scala"
        def _get_master_content(logfile):
            with open(os.path.join(current_file_dir, logfile)) as f:
                return f.read()
        def _get_current_content(logfile):
            with open(os.path.join(current_file_dir, logfile)) as f:
                return f.read()
        config = {
            "master": {
                "file": filename
            },
            "unmerged": {}
        }
        failures = check(config, _get_master_content, _get_current_content, True)
        assert len(failures.keys()) == 0
    
    def test_doesnt_check_if_listed_in_umerged(self):
        filename = "test-files/ExecutorPodsLifecycleManager.scala"
        config = {
            "master": {
                "file": filename
            },
            "unmerged": {
                "file": filename
            }
        }
        failures = check(config, None, None, True)
        assert len(failures.keys()) == 0


if __name__ == '__main__':
    unittest.main()
    
