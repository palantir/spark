#!/usr/bin/env bash

set -o pipefail
set -eu

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
REPO_SPARK_VERSION=$(grep "org.apache.spark:spark-dist_2.11-hadoop-palantir" ${SCRIPT_DIR}/../../versions.props | awk -F'=' '{print $2}' | sed -e 's/^[[:space:]]*//')
echo "Detected that this repository is using spark version $REPO_SPARK_VERSION"
SCRATCH_DIR="${SCRIPT_DIR}/scratch"

WHITELIST_CONFIGS="$(echo ${SCRIPT_DIR}/whitelisted-logs.yml)"
WHITELISTED_SPARK_VERSION=$(head -n 1 ${WHITELIST_CONFIGS} | sed 's/://g')

LINE_NUMBER=2
ERROR_ENCOUNTERED=false
while [ true ]; do
  MAYBE_LOG_LINE=$(sed -n "${LINE_NUMBER}p" ${WHITELIST_CONFIGS} | sed -e 's/^[[:space:]]*//')
  if [[ "${MAYBE_LOG_LINE:0:1}" != '-' ]];
  then 
    break # iterate down the list until we break out of the list
  fi
  ORIGIN=$(echo ${MAYBE_LOG_LINE} | sed 's/-//' | awk -F':' '{print $1}')
  FILEPATH=$(echo ${MAYBE_LOG_LINE} | sed 's/-//' | awk -F':' '{print $2}' | sed 's/\./\//g' | rev | sed 's/\//\./' | rev | sed -e 's/^[[:space:]]*//')
  FILEDIR=$(echo $FILEPATH | sed 's/\/[^\/]*$//')
  FILENAME=$(echo $FILEPATH | sed 's:.*/::')

  CURRENT_FILEPATH=${SCRATCH_DIR}/${REPO_SPARK_VERSION}/$FILEDIR
  WHITELISTED_FILEPATH=${SCRATCH_DIR}/${WHITELISTED_SPARK_VERSION}/$FILEDIR
  mkdir -p ${WHITELISTED_FILEPATH}
  mkdir -p ${CURRENT_FILEPATH}
  curl -s https://api.github.com/repos/palantir/spark/contents/$FILEDIR/$FILENAME?ref=${REPO_SPARK_VERSION} | jq '.content' > ${CURRENT_FILEPATH}/$FILENAME
  curl -s https://api.github.com/repos/palantir/spark/contents/$FILEDIR/$FILENAME?ref=${WHITELISTED_SPARK_VERSION} | jq '.content' > ${WHITELISTED_FILEPATH}/$FILENAME
  if [[ ! -z $(diff ${WHITELISTED_FILEPATH}/$FILENAME ${CURRENT_FILEPATH}/$FILENAME) ]]; then
    echo "* * * WARNING * * *: File "$FILEPATH"/$FILENAME with origin $ORIGIN has changed since it was whitelisted"
    ERROR_ENCOUNTERED=true
  fi
  LINE_NUMBER=$((LINE_NUMBER + 1))
done


echo "Finished analyzing whitelisted log files"
echo "Cleaning up temporary directory."
rm -rf ${SCRATCH_DIR}

if $ERROR_ENCOUNTERED; then
  echo "Whitelisted spark origins correspond to files that have been changed since the last spark version."
  echo "Please check the diffs of those origins to ensure that no newly added log lines leak sensitive information "
  echo "and add those origins as a new entry in the file 'scripts/whitelisted-spark-logs/check-whitelisted-origins-diff.sh'."
  exit 1
else
  exit 0
fi
