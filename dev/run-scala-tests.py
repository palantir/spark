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

import argparse

from test_functions import *


def parse_opts():
    parser = argparse.ArgumentParser()
    parser.add_argument('--modules', nargs='*', default='root')
    parser.add_argument('--excluded-tags', nargs='*')
    parser.add_argument('--included-tags', nargs='*')
    return parser.parse_args()


if __name__ == '__main__':
    opts = parse_opts()
    env = get_build_environment()
    extra_profiles = get_hadoop_profiles(env.hadoop_version)
    test_modules = [m for m in modules.all_modules if m.name in opts.modules]

    assert len(test_modules) == len(opts.modules), "Requested module not found"

    run_scala_tests(
        env.build_tool,
        extra_profiles,
        test_modules,
        opts.excluded_tags or [],
        opts.included_tags or [])
