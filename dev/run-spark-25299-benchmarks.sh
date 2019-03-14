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

#
# Script to create a binary distribution for easy deploys of Spark.
# The distribution directory defaults to dist/ but can be overridden below.
# The distribution contains fat (assembly) jars that include the Scala library,
# so it is completely self contained.
# It does not contain source or *.class files.

set -oue pipefail


function usage {
    echo "Usage: $(basename $0) [-h] [-u]"
    echo ""
    echo "Runs the perf tests and optionally uploads the results as a comment to a PR"
    echo ""
    echo "    -h           help"
    echo "    -u           Upload the perf results as a comment"
    # Exit as error for nesting scripts
    exit 1
}

UPLOAD=false
while getopts "hu" opt; do
  case $opt in
    h)
      usage
      exit 0;;
    u)
      UPLOAD=true;;
  esac
done

echo "Running SPARK-25299 benchmarks"

SPARK_GENERATE_BENCHMARK_FILES=1 ./build/sbt "sql/test:runMain org.apache.spark.shuffle.sort.BlockStoreShuffleReaderBenchmark"
SPARK_GENERATE_BENCHMARK_FILES=1 ./build/sbt "sql/test:runMain org.apache.spark.shuffle.sort.SortShuffleWriterBenchmark"

SPARK_DIR=`pwd`

mkdir -p /tmp/artifacts
cp $SPARK_DIR/sql/core/benchmarks/BlockStoreShuffleReaderBenchmark-results.txt /tmp/artifacts/
cp $SPARK_DIR/sql/core/benchmarks/SortShuffleWriterBenchmark-results.txt /tmp/artifacts/

if [ "$UPLOAD" = false ]; then
    exit 0
fi

IFS=
RESULTS=""
for benchmark_file in /tmp/artifacts/*.txt; do
    RESULTS+=$(cat $benchmark_file)
    RESULTS+=$'\n\n'
done

echo $RESULTS
# Get last git message, filter out empty lines, get the last number of the first line. This is the PR number
PULL_REQUEST_NUM=$(git log -1 --pretty=%B | awk NF | awk '{print $NF}' | head -1 | sed 's/(//g' | sed 's/)//g' | sed 's/#//g')


USERNAME=svc-spark-25299
PASSWORD=$SVC_SPARK_25299_PASSWORD
message='{"body": "```'
message+=$'\n'
message+=$RESULTS
message+=$'\n'
json_message=$(echo $message | awk '{printf "%s\\n", $0}')
json_message+='```", "event":"COMMENT"}' 
echo "$json_message" > benchmark_results.json

echo "Sending benchmark requests to PR $PULL_REQUEST_NUM"
curl -XPOST https://${USERNAME}:${PASSWORD}@api.github.com/repos/palantir/spark/pulls/${PULL_REQUEST_NUM}/reviews -d @benchmark_results.json
rm benchmark_results.json
