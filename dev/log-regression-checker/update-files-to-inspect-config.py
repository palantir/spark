import os
import sys
import yaml

update_type = sys.argv[1]

dir_path = os.path.dirname(os.path.realpath(__file__))
with open(dir_path + "/files-to-inspect.yml") as f:
    try:
        config = yaml.load(stream)
    except yaml.YAMLError as exc:
        print(exc)

def update_on_snapshot(config):
  refs = config.keys().sort()
  if "unmerged" in config:
    refs.get(len(refs) - 1)
    develop

class OrderedVersion:
  self.is_unmerged = False
  self.is_develop = False
  self.major_version = 0
  self.minor_version = 0
  self.bugfix_version = 0
  self.palantir_version = 0

  def __init__(self, string):
    if string == "unmerged":
      self.is_unmerged = True
    self.score = score

     def __lt__(self, other):
         return self.score < other.score
