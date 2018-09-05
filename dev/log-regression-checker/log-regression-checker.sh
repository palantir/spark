#!/usr/bin/env bash

set -o pipefail
set -eu

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
REPO_DIR=${SCRIPT_DIR}/../../

LOG_CONFIGS="$(echo ${SCRIPT_DIR}/files-to-inspect.yml)"


cd ${REPO_DIR}

REF=$(head -n 1 ${LOG_CONFIGS} | sed 's/://g')

LINE_NUMBER=1

## Log lines under the key "unmerged" are exempt
EXEMPT_FILES=""
if [[ ${REF} == *"unmerged"* ]]; then
  LINE_NUMBER=2
  while [ true ]; do
    MAYBE_LOG_LINE=$(sed -n "${LINE_NUMBER}p" ${LOG_CONFIGS} | sed -e 's/^[[:space:]]*//')
    if [[ "${MAYBE_LOG_LINE:0:1}" != '-' ]]; then 
      break # iterate down the list until we find that version
    fi
    FILEPATH=$(echo ${MAYBE_LOG_LINE} | sed 's/-//' | awk -F':' '{print $2}' | sed 's/\./\//g' | rev | sed 's/\//\./' | rev | sed -e 's/^[[:space:]]*//')

    echo "Exempting file since it's listed under unmerged: $FILEPATH"
    EXEMPT_FILES="$EXEMPT_FILES $FILEPATH"
    LINE_NUMBER=$((LINE_NUMBER + 1))
  done
fi

## now, check for diffs between the listed ref and the current commit
REF=$(sed -n "${LINE_NUMBER}p" ${LOG_CONFIGS} | sed -e 's/://g')
LINE_NUMBER=$((LINE_NUMBER + 1))
echo "Checking for file changes against ref $REF"

ERROR_ENCOUNTERED=false

while [ true ]; do
  MAYBE_LOG_LINE=$(sed -n "${LINE_NUMBER}p" ${LOG_CONFIGS} | sed -e 's/^[[:space:]]*//')
  if [[ "${MAYBE_LOG_LINE:0:1}" != '-' ]];
  then 
    break # iterate down the list until we find that version
  fi
  echo $MAYBE_LOG_LINE
  ORIGIN=$(echo ${MAYBE_LOG_LINE} | sed 's/-//' | awk -F':' '{print $1}')
  FILEPATH=$(echo ${MAYBE_LOG_LINE} | sed 's/-//' | awk -F':' '{print $2}' | sed 's/\./\//g' | rev | sed 's/\//\./' | rev | sed -e 's/^[[:space:]]*//')

  if [[ $EXEMPT_FILES == *$FILEPATH* ]]; then
    echo "exempt file $FILEPATH"
    LINE_NUMBER=$((LINE_NUMBER + 1))
    continue
  fi

  if [[ ! -z $(git diff $REF "$FILEPATH") ]]; then
    echo "* * * warning * * *: file "$FILEPATH" with origin $ORIGIN has changed since it was last inspected"
      ERROR_ENCOUNTERED=true
  fi
  LINE_NUMBER=$((LINE_NUMBER + 1))
done


echo "Finished analyzing log files"
echo "Cleaning up temporary directory."

if $ERROR_ENCOUNTERED; then
  echo "Files in files-to-inspect.yml have changed since it was last listed."
  echo "Please check the diffs to ensure that any changes to those files are safe "
  echo "and enter a new entry at the top of the file below under the ref 'unmerged':"
  echo "  $LOG_CONFIGS"
  exit 1
else
  exit 0
fi
