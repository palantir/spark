from log_line_extractor import parse_log_lines

master_branch = "origin/master"

def check(configs, _get_master_contents, _get_current_contents, verbose=False):
    # "unmerged" contains log files to exclude from inspection since they've already been checked and added to the list
    excluded_files = configs["unmerged"].values()

    files_to_check = list()
    for file_to_check in configs["master"].values():
        if file_to_check not in excluded_files:
            files_to_check.append(file_to_check)
 
    print "Checking files in the following list: "
    print '\n'.join(files_to_check)

    failures = dict()
    for log_file in files_to_check:
        expected_log_strings = [x[0] for x in parse_log_lines(_get_master_contents(log_file))]
        current_log_lines = parse_log_lines(_get_current_contents(log_file))
        for log_string, line_number in current_log_lines:
            if log_string not in expected_log_strings:
                if log_file not in failures:
                    failures[log_file] = []
                    print "ERROR: File " + log_file + " contains different logs from those on origin/master"
                if verbose:
                    print "  - Offending log line on line", line_number, ":", log_string
                failures[log_file].append((log_string, line_number))
    if len(failures.keys()) != 0:
        print "Some files in 'files-to-inspect.json' are different on origin/master."
        print "Please check these files and add them to the 'unmerged' section of the file."
    else:
        print "Log regression checker passed."
    return failures

def update_master(configs, verbose=False):
    print "Updating files-to-inspect.json"
    merged_files = configs["unmerged"]
    merged_files.update(configs["master"])
    new_contents = {
        "unmerged": {},
        "master": merged_files
    }
    return new_contents
      
def update_release(configs, archives, tag, verbose=False, dry_run=True):
    print "Updating files-to-inspect-archive.json"
    merged_files = configs["unmerged"]
    merged_files.update(configs["master"])
    archives[tag] = merged_files
    if verbose:
        print "Appending the following if it doesn't already exist:"
        print {tag: merged_files}
    return archives
