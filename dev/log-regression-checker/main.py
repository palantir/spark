import argparse
import os
import json
import subprocess
import re
from log_regression_checker import check, update_master, update_release

def _update_file(contents, write_file, dry_run=True):
    with open(write_file + ".tmp", "w") as f:
        f.write(json.dumps(contents, indent=2))
    if dry_run:
        return    
    shutil.move(write_file + ".tmp", write_file)

parser = argparse.ArgumentParser(description='Process arguments')
parser.add_argument("-c", "--config-file", help="The json file that contains the information about which files to check.", required=True)
parser.add_argument("-a", "--archive-file", help="The json file archiving, for each release version, which files were checked.", required=True)
parser.add_argument("-v", "--verbose", action="store_true")
parser.add_argument("-d", "--dry-run", action="store_true")
args = parser.parse_args()

print os.getcwd()
print args.config_file
current_file_dir = os.path.dirname(os.path.realpath(__file__))
log_config_file = os.path.join(os.getcwd(), args.config_file)
archive_file = os.path.join(os.getcwd(), args.archive_file)
os.chdir(current_file_dir + "/../../")

with open(log_config_file, "r") as f:
    configs = json.load(f)

assert configs["unmerged"] != None, "File files-to-inspect.json is malformatted. Expecting a 'unmerged' entry"
assert configs["master"] != None, "File files-to-inspect.json is malformatted. Expecting a 'master' entry"

tag = subprocess.check_output(['git', 'describe', '--tag']).strip()
is_release_tag = re.match("(\d+).(\d+).(\d+)-palantir.(\d+)$", tag)
branch = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).strip()

if is_release_tag:
    with open(archive_file) as f:
        archives = json.load(f)
    tag = subprocess.check_output(['git', 'describe', '--tags']).strip()
    new_content = update_release(configs, archives, tag, args.verbose)
    _update_file(new_content, archive_file, args.dry_run)
elif branch == 'master':
    new_content = update_master(configs, args.verbose)
    _update_file(new_content, log_config_file, args.dry_run)
else:
    def _get_master_contents(filepath): return subprocess.check_output(['git', 'show', 'origin/master:' + filepath])
    def _get_current_contents(filepath): return subprocess.check_output(['cat', filepath])
    failures = check(configs, _get_master_contents, _get_current_contents, args.verbose)
    if len(failures.keys()) == 0:
        sys.exit(1)
    

