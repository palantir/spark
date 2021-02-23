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

from test_functions import *


if __name__ == '__main__':
    env = get_build_environment()
    extra_profiles = get_hadoop_profiles(env.hadoop_version)

    # ensure we have run the spark build
    build_apache_spark(env.build_tool, extra_profiles)

    # backwards compatibility checks
    if env.build_tool == "sbt":
        # Note: compatibility tests only supported in sbt for now
        detect_binary_inop_with_mima(extra_profiles)
