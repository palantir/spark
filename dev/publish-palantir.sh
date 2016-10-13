#!/usr/bin/env bash

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

set -euo pipefail
version=$(git describe --tags)

make_dist() {
  dist_name="$1"
  build_flags="$2"
  file_name="spark-${version}-bin-${dist_name}.tgz"
  sudo apt-get --assume-yes install r-base r-base-dev
  ./dev/make-distribution.sh --name $dist_name --tgz $build_flags
  curl -u $BINTRAY_USERNAME:$BINTRAY_PASSWORD -T $file_name "https://api.bintray.com/content/palantir/releases/spark/${version}/org/apache/spark/dist/${file_name}"
}

publish_artifacts() {
  tmp_settings="tmp-settings.xml"
  echo "<settings><servers><server>" > $tmp_settings
  echo "<id>bintray-palantir-release</id><username>$BINTRAY_USERNAME</username>" >> $tmp_settings
  echo "<password>$BINTRAY_PASSWORD</password>" >> $tmp_settings
  echo "</server></servers></settings>" >> $tmp_settings

  ./build/mvn versions:set -DnewVersion=$version
  ./build/mvn --settings $tmp_settings -DskipTests -Phadoop-2.7 -Pmesos -Pkinesis-asl -Pyarn -Phive-thriftserver clean deploy
}

make_dist hadoop-2.7 "-Phadoop-2.7 -Pmesos -Pkinesis-asl -Pyarn -Phive-thriftserver -Phive"
make_dist without-hadoop "-Psparkr -Phadoop-provided -Pyarn -Pmesos"

publish_artifacts

curl -u $BINTRAY_USERNAME:$BINTRAY_PASSWORD -X POST https://api.bintray.com/content/palantir/releases/spark/$(git describe --tags)/publish
