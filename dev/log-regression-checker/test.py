import unittest
import os
from log_regression_checker import *
from log_line_extractor import *

current_file_dir = os.path.dirname(os.path.realpath(__file__))

class TestStringMethods(unittest.TestCase):
    def test_parse_ExecutorPodsLifecycleManager(self):
        expected_lines = [
            "    logInfo(s\"Snapshot reported deleted executor with id $execId,\" +\n" +
            "        s\" pod name ${state.pod.getMetadata.getName}\")",
            "    logDebug(s\"Snapshot reported failed executor with id $execId,\" +\n" +
            "        s\" pod name ${state.pod.getMetadata.getName}\")",
            "    logDebug(s\"Snapshot reported succeeded executor with id $execId,\" +\n" +
            "        s\" pod name ${state.pod.getMetadata.getName}. Note that succeeded executors are\" +\n" +
            "        s\" unusual unless Spark specifically informed the executor to exit.\")",
            "    logDebug(exitReasonMessage)", # test no strings
            "    logDebug(s\"Removed executors with ids ${execIdsRemovedInThisRound.mkString(\",\")}\" +\n" +
            "        s\" from Spark that were either found to be deleted or non-existent in the cluster.\")"
        ]
        with open(os.path.join(current_file_dir, "test-files/ExecutorPodsLifecycleManager.scala")) as f:
            content = f.read()
        log_lines = parse_out_log_lines(content)
        assert len(log_lines) == 5
        for line in log_lines:
            assert line in expected_lines

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

        expected_errors = [
            "    logDebug(s\"Snapshot reported deleted executor with id $execId,\" +\n" +
            "        s\" pod name ${state.pod.getMetadata.getName}\")",
            "    logDebug(s\"Snapshot reported failed executor with id $execId,\" +\n" +
            "        s\" pod name ${state.pod.getMetadata.getName} blah blah\")",
            "    logError(\"Unexpected log here\")"
            ]
        assert (filename in failures)
        assert len(failures.keys()) == 1
        assert len(failures[filename]) == 3
        for error in failures[filename]:
            assert error in expected_errors

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
    
