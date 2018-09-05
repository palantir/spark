import os
import sys
import argparse
import re
import subprocess
import json
import shutil

current_file_dir = os.path.dirname(os.path.realpath(__file__))
log_config_file = os.path.join(current_file_dir, "files-to-inspect.json")
archive_file = os.path.join(current_file_dir, "files-to-inspect-archive.json")
os.chdir(current_file_dir + "/../../")

def check(configs, verbose=False):
    # "unmerged" contains log files to exclude from inspection since they've already been checked and added to the list
    excluded_files = configs["unmerged"].values()

    files_to_check = list()
    for file_to_check in configs["master"].values():
        if file_to_check not in excluded_files:
            files_to_check.append(file_to_check)
 
    print "Checking files in the following list: "
    print '\n'.join(files_to_check)

    success = True
    for log_file in files_to_check:
        output = subprocess.check_output(['git', 'diff', 'master', log_file])
        if output != "":
            success = False
            print "ERROR: Log file " + log_file + " is different from what's on master"
            if verbose:
                print output
    if not success:
        print "Some files in 'files-to-inspect.json' are different on master."
        print "Please check these files and add them to the 'unmerged' section of the file."
    else:
        print "Log regression checker passed."
    return success


def update_master(configs, verbose=False, dry_run=True):
    print "Updating files-to-inspect.json"
    merged_files = configs["unmerged"]
    merged_files.update(configs["master"])
    new_contents = {
        "unmerged": {},
        "master": merged_files
    }
      
    with open(log_config_file + ".tmp", "w") as f:
        f.write(json.dumps(new_contents, indent=2))
    if dry_run:
        return
    shutil.move(log_config_file + ".tmp", log_config_file)

def update_release(configs, verbose=False, dry_run=True):
    print "Updating files-to-inspect-archive.json"
    merged_files = configs["unmerged"]
    merged_files.update(configs["master"])
    tag = subprocess.check_output(['git', 'describe', '--tags']).strip()
    with open(archive_file) as f:
        archives = json.load(f)
    archives[tag] = merged_files
    if verbose:
        print "Appending the following if it doesn't already exist:"
        print {tag: merged_files}
    with open(archive_file + ".tmp", "w") as f:
        f.write(json.dumps(archives, indent=2, sort_keys=True))
    if dry_run:
        return
    shutil.move(archive_file + ".tmp", archive_file)
  

parser = argparse.ArgumentParser(description='Process arguments')
parser.add_argument("operation", choices=["check", "update-master", "update-release"], help="The type of operation. Can be 'check' or 'update'")
parser.add_argument("-v", "--verbose", action="store_true")
parser.add_argument("-d", "--dry-run", action="store_true")
args = parser.parse_args()

with open(log_config_file, "r") as f:
    configs = json.load(f)

assert configs["unmerged"] != None, "File files-to-inspect.json is malformatted. Expecting a 'unmerged' entry"
assert configs["master"] != None, "File files-to-inspect.json is malformatted. Expecting a 'master' entry"

if args.operation == "check":
    success = check(configs, args.verbose)
    if not success:
      sys.exit(1)
elif args.operation == "update-master":
    update_master(configs, args.verbose, args.dry_run)
else:
    update_release(configs, args.verbose, args.dry_run)

  
