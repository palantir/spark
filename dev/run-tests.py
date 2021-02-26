#!/usr/bin/env python3

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import itertools
from argparse import ArgumentParser
import os
import random
import re
import sys
import subprocess
import glob
import shutil
from collections import namedtuple

from sparktestsupport import SPARK_HOME, USER_HOME, ERROR_CODES
from sparktestsupport.shellutils import exit_from_command_with_retcode, run_cmd, rm_r, which
from sparktestsupport.toposort import toposort_flatten
import sparktestsupport.modules as modules


# -------------------------------------------------------------------------------------------------
# Functions for traversing module dependency graph
# -------------------------------------------------------------------------------------------------


def determine_modules_for_files(filenames):
    """
    Given a list of filenames, return the set of modules that contain those files.
    If a file is not associated with a more specific submodule, then this method will consider that
    file to belong to the 'root' module. `.github` directory is counted only in GitHub Actions,
    and `appveyor.yml` is always ignored because this file is dedicated only to AppVeyor builds.

    >>> sorted(x.name for x in determine_modules_for_files(["python/pyspark/a.py", "sql/core/foo"]))
    ['pyspark-core', 'sql']
    >>> [x.name for x in determine_modules_for_files(["file_not_matched_by_any_subproject"])]
    ['root']
    >>> [x.name for x in determine_modules_for_files(["appveyor.yml"])]
    []
    """
    changed_modules = set()
    for filename in filenames:
        if filename in ("appveyor.yml",):
            continue
        if ("GITHUB_ACTIONS" not in os.environ) and filename.startswith(".github"):
            continue
        matched_at_least_one_module = False
        for module in modules.all_modules:
            if module.contains_file(filename):
                changed_modules.add(module)
                matched_at_least_one_module = True
        if not matched_at_least_one_module:
            changed_modules.add(modules.root)
    return changed_modules


def identify_changed_files_from_git_commits(patch_sha, target_branch=None, target_ref=None):
    """
    Given a git commit and target ref, use the set of files changed in the diff in order to
    determine which modules' tests should be run.

    >>> [x.name for x in determine_modules_for_files( \
            identify_changed_files_from_git_commits("fc0a1475ef", target_ref="5da21f07"))]
    ['graphx']
    >>> 'root' in [x.name for x in determine_modules_for_files( \
         identify_changed_files_from_git_commits("50a0496a43", target_ref="6765ef9"))]
    True
    """
    if target_branch is None and target_ref is None:
        raise AttributeError("must specify either target_branch or target_ref")
    elif target_branch is not None and target_ref is not None:
        raise AttributeError("must specify either target_branch or target_ref, not both")
    if target_branch is not None:
        diff_target = target_branch
        run_cmd(['git', 'fetch', 'origin', str(target_branch+':'+target_branch)])
    else:
        diff_target = target_ref
    raw_output = subprocess.check_output(['git', 'diff', '--name-only', patch_sha, diff_target],
                                         universal_newlines=True)
    # Remove any empty strings
    return [f for f in raw_output.split('\n') if f]


def setup_test_environ(environ):
    print("[info] Setup the following environment variables for tests: ")
    for (k, v) in environ.items():
        print("%s=%s" % (k, v))
        os.environ[k] = v


def determine_modules_to_test(changed_modules, deduplicated=True):
    """
    Given a set of modules that have changed, compute the transitive closure of those modules'
    dependent modules in order to determine the set of modules that should be tested.

    Returns a topologically-sorted list of modules (ties are broken by sorting on module names).
    If ``deduplicated`` is disabled, the modules are returned without tacking the deduplication
    by dependencies into account.

    >>> [x.name for x in determine_modules_to_test([modules.root])]
    ['root']
    >>> [x.name for x in determine_modules_to_test([modules.build])]
    ['root']
    >>> [x.name for x in determine_modules_to_test([modules.core])]
    ['root']
    >>> [x.name for x in determine_modules_to_test([modules.launcher])]
    ['root']
    >>> [x.name for x in determine_modules_to_test([modules.graphx])]
    ['graphx', 'examples']
    >>> [x.name for x in determine_modules_to_test([modules.sql])]
    ... # doctest: +NORMALIZE_WHITESPACE
    ['sql', 'avro', 'hive', 'mllib', 'sql-kafka-0-10', 'examples', 'hive-thriftserver',
     'pyspark-sql', 'repl', 'sparkr', 'pyspark-mllib', 'pyspark-ml']
    >>> sorted([x.name for x in determine_modules_to_test(
    ...     [modules.sparkr, modules.sql], deduplicated=False)])
    ... # doctest: +NORMALIZE_WHITESPACE
    ['avro', 'examples', 'hive', 'hive-thriftserver', 'mllib', 'pyspark-ml',
     'pyspark-mllib', 'pyspark-sql', 'repl', 'sparkr', 'sql', 'sql-kafka-0-10']
    >>> sorted([x.name for x in determine_modules_to_test(
    ...     [modules.sql, modules.core], deduplicated=False)])
    ... # doctest: +NORMALIZE_WHITESPACE
    ['avro', 'catalyst', 'core', 'examples', 'graphx', 'hive', 'hive-thriftserver',
     'mllib', 'mllib-local', 'pyspark-core', 'pyspark-ml', 'pyspark-mllib',
     'pyspark-sql', 'pyspark-streaming', 'repl', 'root',
     'sparkr', 'sql', 'sql-kafka-0-10', 'streaming', 'streaming-kafka-0-10',
     'streaming-kinesis-asl']
    """
    modules_to_test = set()
    for module in changed_modules:
        modules_to_test = modules_to_test.union(
            determine_modules_to_test(module.dependent_modules, deduplicated))
    modules_to_test = modules_to_test.union(set(changed_modules))

    if not deduplicated:
        return modules_to_test

    # If we need to run all of the tests, then we should short-circuit and return 'root'
    if modules.root in modules_to_test:
        return [modules.root]
    return toposort_flatten(
        {m: set(m.dependencies).intersection(modules_to_test) for m in modules_to_test}, sort=True)


def determine_tags_to_exclude(changed_modules):
    tags = []
    for m in modules.all_modules:
        if m not in changed_modules:
            tags += m.test_tags
    return tags


# -------------------------------------------------------------------------------------------------
# Functions for working with subprocesses and shell tools
# -------------------------------------------------------------------------------------------------


def determine_java_executable():
    """Will return the path of the java executable that will be used by Spark's
    tests or `None`"""

    # Any changes in the way that Spark's build detects java must be reflected
    # here. Currently the build looks for $JAVA_HOME/bin/java then falls back to
    # the `java` executable on the path

    java_home = os.environ.get("JAVA_HOME")

    # check if there is an executable at $JAVA_HOME/bin/java
    java_exe = which(os.path.join(java_home, "bin", "java")) if java_home else None
    # if the java_exe wasn't set, check for a `java` version on the $PATH
    return java_exe if java_exe else which("java")


# -------------------------------------------------------------------------------------------------
# Functions for running the other build and test scripts
# -------------------------------------------------------------------------------------------------


def set_title_and_block(title, err_block):
    os.environ["CURRENT_BLOCK"] = str(ERROR_CODES[err_block])
    line_str = '=' * 72

    print('')
    print(line_str)
    print(title)
    print(line_str)


def run_apache_rat_checks():
    set_title_and_block("Running Apache RAT checks", "BLOCK_RAT")
    run_cmd([os.path.join(SPARK_HOME, "dev", "check-license")])


def run_scala_style_checks(extra_profiles):
    build_profiles = extra_profiles + modules.root.build_profile_flags
    set_title_and_block("Running Scala style checks", "BLOCK_SCALA_STYLE")
    profiles = " ".join(build_profiles)
    print("[info] Checking Scala style using SBT with these profiles: ", profiles)
    run_cmd([os.path.join(SPARK_HOME, "dev", "lint-scala"), profiles])


def run_java_style_checks(build_profiles):
    set_title_and_block("Running Java style checks", "BLOCK_JAVA_STYLE")
    # The same profiles used for building are used to run Checkstyle by SBT as well because
    # the previous build looks reused for Checkstyle and affecting Checkstyle. See SPARK-27130.
    profiles = " ".join(build_profiles)
    print("[info] Checking Java style using SBT with these profiles: ", profiles)
    run_cmd([os.path.join(SPARK_HOME, "dev", "sbt-checkstyle"), profiles])


def run_python_style_checks():
    set_title_and_block("Running Python style checks", "BLOCK_PYTHON_STYLE")
    run_cmd([os.path.join(SPARK_HOME, "dev", "lint-python")])


def run_sparkr_style_checks():
    set_title_and_block("Running R style checks", "BLOCK_R_STYLE")

    if which("R"):
        # R style check should be executed after `install-dev.sh`.
        # Since warnings about `no visible global function definition` appear
        # without the installation. SEE ALSO: SPARK-9121.
        run_cmd([os.path.join(SPARK_HOME, "dev", "lint-r")])
    else:
        print("Ignoring SparkR style check as R was not found in PATH")


def build_spark_documentation():
    set_title_and_block("Building Spark Documentation", "BLOCK_DOCUMENTATION")
    os.environ["PRODUCTION"] = "1 jekyll build"

    os.chdir(os.path.join(SPARK_HOME, "docs"))

    jekyll_bin = which("jekyll")

    if not jekyll_bin:
        print("[error] Cannot find a version of `jekyll` on the system; please",
              " install one and retry to build documentation.")
        sys.exit(int(os.environ.get("CURRENT_BLOCK", 255)))
    else:
        run_cmd([jekyll_bin, "build"])

    os.chdir(SPARK_HOME)


def get_zinc_port():
    """
    Get a randomized port on which to start Zinc
    """
    return random.randrange(3030, 4030)


def exec_maven(mvn_args=()):
    """Will call Maven in the current directory with the list of mvn_args passed
    in and returns the subprocess for any further processing"""

    zinc_port = get_zinc_port()
    os.environ["ZINC_PORT"] = "%s" % zinc_port
    zinc_flag = "-DzincPort=%s" % zinc_port
    flags = [os.path.join(SPARK_HOME, "build", "mvn"), zinc_flag]
    run_cmd(flags + mvn_args)


def exec_sbt(sbt_args=()):
    """Will call SBT in the current directory with the list of mvn_args passed
    in and returns the subprocess for any further processing"""

    sbt_cmd = [os.path.join(SPARK_HOME, "build", "sbt")] + sbt_args

    sbt_output_filter = re.compile(b"^.*[info].*Resolving" + b"|" +
                                   b"^.*[warn].*Merging" + b"|" +
                                   b"^.*[info].*Including")

    # NOTE: echo "q" is needed because sbt on encountering a build file
    # with failure (either resolution or compilation) prompts the user for
    # input either q, r, etc to quit or retry. This echo is there to make it
    # not block.
    echo_proc = subprocess.Popen(["echo", "\"q\n\""], stdout=subprocess.PIPE)
    sbt_proc = subprocess.Popen(sbt_cmd,
                                stdin=echo_proc.stdout,
                                stdout=subprocess.PIPE)
    echo_proc.wait()
    for line in iter(sbt_proc.stdout.readline, b''):
        if not sbt_output_filter.match(line):
            print(line.decode('utf-8'), end='')
    retcode = sbt_proc.wait()

    if retcode != 0:
        exit_from_command_with_retcode(sbt_cmd, retcode)


def get_hadoop_profiles(hadoop_version):
    """
    For the given Hadoop version tag, return a list of Maven/SBT profile flags for
    building and testing against that Hadoop version.
    """

    sbt_maven_hadoop_profiles = {
        "hadoop2.7": ["-Phadoop-2.7"],
        "hadoop3.2": ["-Phadoop-3.2"],
        "hadooppalantir": ["-Phadoop-palantir"],
    }

    if hadoop_version in sbt_maven_hadoop_profiles:
        return sbt_maven_hadoop_profiles[hadoop_version]
    else:
        print("[error] Could not find", hadoop_version, "in the list. Valid options",
              " are", sbt_maven_hadoop_profiles.keys())
        sys.exit(int(os.environ.get("CURRENT_BLOCK", 255)))


def get_hive_profiles(hive_version):
    """
    For the given Hive version tag, return a list of Maven/SBT profile flags for
    building and testing against that Hive version.
    """

    sbt_maven_hive_profiles = {
        "hive1.2": ["-Phive-1.2"],
        "hive2.3": ["-Phive-2.3"],
    }

    if hive_version in sbt_maven_hive_profiles:
        return sbt_maven_hive_profiles[hive_version]
    else:
        print("[error] Could not find", hive_version, "in the list. Valid options",
              " are", sbt_maven_hive_profiles.keys())
        sys.exit(int(os.environ.get("CURRENT_BLOCK", 255)))


def build_spark_maven(extra_profiles):
    # Enable all of the profiles for the build:
    build_profiles = extra_profiles + modules.root.build_profile_flags
    mvn_goals = ["clean", "package", "-DskipTests"]
    profiles_and_goals = build_profiles + mvn_goals

    print("[info] Building Spark using Maven with these arguments: ", " ".join(profiles_and_goals))

    exec_maven(profiles_and_goals)


def build_spark_sbt(extra_profiles):
    # Enable all of the profiles for the build:
    build_profiles = extra_profiles + modules.root.build_profile_flags
    sbt_goals = ["test:package",  # Build test jars as some tests depend on them
                 "streaming-kinesis-asl-assembly/assembly"]
    profiles_and_goals = build_profiles + sbt_goals

    print("[info] Building Spark using SBT with these arguments: ", " ".join(profiles_and_goals))

    exec_sbt(profiles_and_goals)


def build_spark_unidoc_sbt(extra_profiles):
    set_title_and_block("Building Unidoc API Documentation", "BLOCK_DOCUMENTATION")
    # Enable all of the profiles for the build:
    build_profiles = extra_profiles + modules.root.build_profile_flags
    sbt_goals = ["unidoc"]
    profiles_and_goals = build_profiles + sbt_goals

    print("[info] Building Spark unidoc using SBT with these arguments: ",
          " ".join(profiles_and_goals))

    exec_sbt(profiles_and_goals)


def build_spark_assembly_sbt(extra_profiles, checkstyle=False):
    # Enable all of the profiles for the build:
    build_profiles = extra_profiles + modules.root.build_profile_flags
    sbt_goals = ["assembly/package"]
    profiles_and_goals = build_profiles + sbt_goals
    print("[info] Building Spark assembly using SBT with these arguments: ",
          " ".join(profiles_and_goals))
    exec_sbt(profiles_and_goals)

    if checkstyle:
        run_java_style_checks(build_profiles)

    build_spark_unidoc_sbt(extra_profiles)


def build_apache_spark(build_tool, extra_profiles):
    """Will build Spark with the extra profiles and the passed in build tool
    (either `sbt` or `maven`). Defaults to using `sbt`."""

    set_title_and_block("Building Spark", "BLOCK_BUILD")

    rm_r("lib_managed")

    if build_tool == "maven":
        build_spark_maven(extra_profiles)
    else:
        build_spark_sbt(extra_profiles)


def detect_binary_inop_with_mima(extra_profiles):
    build_profiles = extra_profiles + modules.root.build_profile_flags
    set_title_and_block("Detecting binary incompatibilities with MiMa", "BLOCK_MIMA")
    profiles = " ".join(build_profiles)
    print("[info] Detecting binary incompatibilities with MiMa using SBT with these profiles: ",
          profiles)
    run_cmd([os.path.join(SPARK_HOME, "dev", "mima"), profiles])


def run_scala_tests_maven(test_profiles):
    mvn_test_goals = ["test", "--fail-at-end"]

    profiles_and_goals = test_profiles + mvn_test_goals

    print("[info] Running Spark tests using Maven with these arguments: ",
          " ".join(profiles_and_goals))

    exec_maven(profiles_and_goals)


def run_scala_tests_sbt(test_modules, test_profiles):
    sbt_test_goals = list(itertools.chain.from_iterable(m.sbt_test_goals for m in test_modules))

    if 'CIRCLE_TEST_REPORTS' in os.environ:
        # The test task in the circle configuration runs only the appropriate test for the current
        # circle node, then copies the results to CIRCLE_TEST_REPORTS.
        # We are not worried about running only the `test_modules`, since we always run the whole
        # suite in circle anyway.
        sbt_test_goals = ['circle:test']

    if not sbt_test_goals:
        return

    profiles_and_goals = test_profiles + sbt_test_goals

    print("[info] Running Spark tests using SBT with these arguments: ",
          " ".join(profiles_and_goals))

    exec_sbt(profiles_and_goals)


def run_scala_tests(build_tool, extra_profiles, test_modules, excluded_tags, included_tags):
    """Function to properly execute all tests passed in as a set from the
    `determine_test_suites` function"""
    set_title_and_block("Running Spark unit tests", "BLOCK_SPARK_UNIT_TESTS")

    test_modules = set(test_modules)

    test_profiles = extra_profiles + \
        list(set(itertools.chain.from_iterable(m.build_profile_flags for m in test_modules)))

    if included_tags:
        test_profiles += ['-Dtest.include.tags=' + ",".join(included_tags)]
    if excluded_tags:
        test_profiles += ['-Dtest.exclude.tags=' + ",".join(excluded_tags)]

    # set up java11 env if this is a pull request build with 'test-java11' in the title
    if "ghprbPullTitle" in os.environ:
        if "test-java11" in os.environ["ghprbPullTitle"].lower():
            os.environ["JAVA_HOME"] = "/usr/java/jdk-11.0.1"
            os.environ["PATH"] = "%s/bin:%s" % (os.environ["JAVA_HOME"], os.environ["PATH"])
            test_profiles += ['-Djava.version=11']

    if build_tool == "maven":
        run_scala_tests_maven(test_profiles)
    else:
        run_scala_tests_sbt(test_modules, test_profiles)


def run_python_tests(test_modules, parallelism, python_executables=None, with_coverage=False):
    set_title_and_block("Running PySpark tests", "BLOCK_PYSPARK_UNIT_TESTS")

    if with_coverage:
        # Coverage makes the PySpark tests flaky due to heavy parallelism.
        # When we run PySpark tests with coverage, it uses 4 for now as
        # workaround.
        parallelism = 4
        script = "run-tests-with-coverage"
    else:
        script = "run-tests"
    command = [os.path.join(SPARK_HOME, "python", script)]
    if test_modules != [modules.root]:
        command.append("--modules=%s" % ','.join(m.name for m in test_modules))
    if python_executables is not None:
        command.append("--python-executables={}".format(",".join(python_executables)))
    command.append("--parallelism=%i" % parallelism)
    command.append("--verbose")
    run_cmd(command)

    if with_coverage:
        post_python_tests_results()


def post_python_tests_results():
    if "SPARK_TEST_KEY" not in os.environ:
        print("[error] 'SPARK_TEST_KEY' environment variable was not set. Unable to post "
              "PySpark coverage results.")
        sys.exit(1)
    spark_test_key = os.environ.get("SPARK_TEST_KEY")
    # The steps below upload HTMLs to 'github.com/spark-test/pyspark-coverage-site'.
    # 1. Clone PySpark coverage site.
    run_cmd([
        "git",
        "clone",
        "https://spark-test:%s@github.com/spark-test/pyspark-coverage-site.git" % spark_test_key])
    # 2. Remove existing HTMLs.
    run_cmd(["rm", "-fr"] + glob.glob("pyspark-coverage-site/*"))
    # 3. Copy generated coverage HTMLs.
    for f in glob.glob("%s/python/test_coverage/htmlcov/*" % SPARK_HOME):
        shutil.copy(f, "pyspark-coverage-site/")
    os.chdir("pyspark-coverage-site")
    try:
        # 4. Check out to a temporary branch.
        run_cmd(["git", "symbolic-ref", "HEAD", "refs/heads/latest_branch"])
        # 5. Add all the files.
        run_cmd(["git", "add", "-A"])
        # 6. Commit current HTMLs.
        run_cmd([
            "git",
            "commit",
            "-am",
            "Coverage report at latest commit in Apache Spark",
            '--author="Apache Spark Test Account <sparktestacc@gmail.com>"'])
        # 7. Delete the old branch.
        run_cmd(["git", "branch", "-D", "gh-pages"])
        # 8. Rename the temporary branch to master.
        run_cmd(["git", "branch", "-m", "gh-pages"])
        # 9. Finally, force update to our repository.
        run_cmd(["git", "push", "-f", "origin", "gh-pages"])
    finally:
        os.chdir("..")
        # 10. Remove the cloned repository.
        shutil.rmtree("pyspark-coverage-site")


def run_python_packaging_tests(use_conda, python_versions=None):
    set_title_and_block("Running PySpark packaging tests", "BLOCK_PYSPARK_PIP_TESTS")
    command = [os.path.join(SPARK_HOME, "dev", "run-pip-tests")]
    env = dict(os.environ)
    if python_versions is not None:
        env["PYTHON_EXECS_IN"] = ";".join(python_versions)
        if use_conda:
            env["USE_CONDA"] = "1"
    run_cmd(command)


def run_build_tests():
    set_title_and_block("Running build tests", "BLOCK_BUILD_TESTS")
    run_cmd([os.path.join(SPARK_HOME, "dev", "test-dependencies.sh")])


def run_sparkr_tests():
    set_title_and_block("Running SparkR tests", "BLOCK_SPARKR_UNIT_TESTS")

    if which("R"):
        run_cmd([os.path.join(SPARK_HOME, "R", "run-tests.sh")])
    else:
        print("Ignoring SparkR tests as R was not found in PATH")


def parse_opts():
    parser = ArgumentParser(
        prog="run-tests"
    )
    parser.add_argument(
        "-p", "--parallelism", type=int, default=8,
        help="The number of suites to test in parallel (default %(default)d)"
    )
    parser.add_argument(
        "-m", "--modules", type=str,
        default=None,
        help="A comma-separated list of modules to test "
             "(default: %s)" % ",".join(sorted([m.name for m in modules.all_modules]))
    )
    parser.add_argument(
        "-e", "--excluded-tags", type=str,
        default=None,
        help="A comma-separated list of tags to exclude in the tests, "
             "e.g., org.apache.spark.tags.ExtendedHiveTest "
    )
    parser.add_argument(
        "-i", "--included-tags", type=str,
        default=None,
        help="A comma-separated list of tags to include in the tests, "
             "e.g., org.apache.spark.tags.ExtendedHiveTest "
    )

    args, unknown = parser.parse_known_args()
    if unknown:
        parser.error("Unsupported arguments: %s" % ' '.join(unknown))
    if args.parallelism < 1:
        parser.error("Parallelism cannot be less than 1")
    return args


def main():
    opts = parse_opts()
    # Ensure the user home directory (HOME) is valid and is an absolute directory
    if not USER_HOME or not os.path.isabs(USER_HOME):
        print("[error] Cannot determine your home directory as an absolute path;",
              " ensure the $HOME environment variable is set properly.")
        sys.exit(1)

    os.chdir(SPARK_HOME)

    rm_r(os.path.join(SPARK_HOME, "work"))
    rm_r(os.path.join(USER_HOME, ".ivy2", "local", "org.apache.spark"))
    rm_r(os.path.join(USER_HOME, ".ivy2", "cache", "org.apache.spark"))

    os.environ["CURRENT_BLOCK"] = str(ERROR_CODES["BLOCK_GENERAL"])

    java_exe = determine_java_executable()

    if not java_exe:
        print("[error] Cannot find a version of `java` on the system; please",
              " install one and retry.")
        sys.exit(2)

    # Install SparkR
    should_only_test_modules = opts.modules is not None
    test_modules = []
    if should_only_test_modules:
        str_test_modules = [m.strip() for m in opts.modules.split(",")]
        test_modules = [m for m in modules.all_modules if m.name in str_test_modules]

    if not should_only_test_modules or modules.sparkr in test_modules:
        # If tests modules are specified, we will not run R linter.
        # SparkR needs the manual SparkR installation.
        if which("R"):
            run_cmd([os.path.join(SPARK_HOME, "R", "install-dev.sh")])
        else:
            print("Cannot install SparkR as R was not found in PATH")

    if os.environ.get("AMPLAB_JENKINS"):
        # if we're on the Amplab Jenkins build servers setup variables
        # to reflect the environment settings
        build_tool = os.environ.get("AMPLAB_JENKINS_BUILD_TOOL", "sbt")
        hadoop_version = os.environ.get("AMPLAB_JENKINS_BUILD_PROFILE", "hadoop2.7")
        hive_version = os.environ.get("AMPLAB_JENKINS_BUILD_HIVE_PROFILE", "hive2.3")
        test_env = "amplab_jenkins"
        # add path for Python3 in Jenkins if we're calling from a Jenkins machine
        # TODO(sknapp):  after all builds are ported to the ubuntu workers, change this to be:
        # /home/jenkins/anaconda2/envs/py36/bin
        os.environ["PATH"] = "/home/anaconda/envs/py36/bin:" + os.environ.get("PATH")
    else:
        # else we're running locally or Github Actions.
        build_tool = "sbt"
        hadoop_version = os.environ.get("HADOOP_PROFILE", "hadoop2.7")
        hive_version = os.environ.get("HIVE_PROFILE", "hive2.3")
        if "GITHUB_ACTIONS" in os.environ:
            test_env = "github_actions"
        else:
            test_env = "local"

    print("[info] Using build tool", build_tool, "with Hadoop profile", hadoop_version,
          "and Hive profile", hive_version, "under environment", test_env)
    extra_profiles = get_hadoop_profiles(hadoop_version) + get_hive_profiles(hive_version)

    changed_modules = []
    changed_files = []
    included_tags = []
    excluded_tags = []
    if should_only_test_modules:
        # If we're running the tests in Github Actions, attempt to detect and test
        # only the affected modules.
        if test_env == "github_actions":
            if os.environ["GITHUB_BASE_REF"] != "":
                # Pull requests
                changed_files = identify_changed_files_from_git_commits(
                    os.environ["GITHUB_SHA"], target_branch=os.environ["GITHUB_BASE_REF"])
            else:
                # Build for each commit.
                changed_files = identify_changed_files_from_git_commits(
                    os.environ["GITHUB_SHA"], target_ref=os.environ["GITHUB_PREV_SHA"])

            modules_to_test = determine_modules_to_test(
                determine_modules_for_files(changed_files), deduplicated=False)

            if modules.root not in modules_to_test:
                # If root module is not found, only test the intersected modules.
                # If root module is found, just run the modules as specified initially.
                test_modules = list(set(modules_to_test).intersection(test_modules))

        changed_modules = test_modules
        if len(changed_modules) == 0:
            print("[info] There are no modules to test, exiting without testing.")
            return

    # If we're running the tests in AMPLab Jenkins, calculate the diff from the targeted branch, and
    # detect modules to test.
    elif test_env == "amplab_jenkins" and os.environ.get("AMP_JENKINS_PRB"):
        target_branch = os.environ["ghprbTargetBranch"]
        changed_files = identify_changed_files_from_git_commits("HEAD", target_branch=target_branch)
        changed_modules = determine_modules_for_files(changed_files)
        test_modules = determine_modules_to_test(changed_modules)
        excluded_tags = determine_tags_to_exclude(changed_modules)

    # If there is no changed module found, tests all.
    if not changed_modules:
        changed_modules = [modules.root]
    if not test_modules:
        test_modules = determine_modules_to_test(changed_modules)

    if opts.excluded_tags:
        excluded_tags.extend([t.strip() for t in opts.excluded_tags.split(",")])
    if opts.included_tags:
        included_tags.extend([t.strip() for t in opts.included_tags.split(",")])

    print("[info] Found the following changed modules:",
          ", ".join(x.name for x in changed_modules))

    # setup environment variables
    # note - the 'root' module doesn't collect environment variables for all modules. Because the
    # environment variables should not be set if a module is not changed, even if running the 'root'
    # module. So here we should use changed_modules rather than test_modules.
    test_environ = {}
    for m in changed_modules:
        test_environ.update(m.environ)
    setup_test_environ(test_environ)

    should_run_java_style_checks = False
    if not should_only_test_modules:
        # license checks
        run_apache_rat_checks()

        # style checks
        if not changed_files or any(f.endswith(".scala")
                                    or f.endswith("scalastyle-config.xml")
                                    for f in changed_files):
            run_scala_style_checks(extra_profiles)
        if not changed_files or any(f.endswith(".java")
                                    or f.endswith("checkstyle.xml")
                                    or f.endswith("checkstyle-suppressions.xml")
                                    for f in changed_files):
            # Run SBT Checkstyle after the build to prevent a side-effect to the build.
            should_run_java_style_checks = True
        if not changed_files or any(f.endswith("lint-python")
                                    or f.endswith("tox.ini")
                                    or f.endswith(".py")
                                    for f in changed_files):
            run_python_style_checks()
        if not changed_files or any(f.endswith(".R")
                                    or f.endswith("lint-r")
                                    or f.endswith(".lintr")
                                    for f in changed_files):
            run_sparkr_style_checks()

    # determine if docs were changed and if we're inside the amplab environment
    # note - the below commented out until *all* Jenkins workers can get `jekyll` installed
    # if "DOCS" in changed_modules and test_env == "amplab_jenkins":
    #    build_spark_documentation()

    if any(m.should_run_build_tests for m in test_modules):
        run_build_tests()

    # spark build
    build_apache_spark(build_tool, extra_profiles)

    # backwards compatibility checks
    if build_tool == "sbt":
        # Note: compatibility tests only supported in sbt for now
        detect_binary_inop_with_mima(extra_profiles)
        # Since we did not build assembly/package before running dev/mima, we need to
        # do it here because the tests still rely on it; see SPARK-13294 for details.
        build_spark_assembly_sbt(extra_profiles, should_run_java_style_checks)

    # run the test suites
    run_scala_tests(build_tool, extra_profiles, test_modules, excluded_tags, included_tags)

    modules_with_python_tests = [m for m in test_modules if m.python_test_goals]
    if modules_with_python_tests:
        # We only run PySpark tests with coverage report in one specific job with
        # Spark master with SBT in Jenkins.
        is_sbt_master_job = "SPARK_MASTER_SBT_HADOOP_2_7" in os.environ
        run_python_tests(
            modules_with_python_tests, opts.parallelism, with_coverage=is_sbt_master_job)
        run_python_packaging_tests()
    if any(m.should_run_r_tests for m in test_modules):
        run_sparkr_tests()


def _test():
    import doctest
    failure_count = doctest.testmod()[0]
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
    main()
