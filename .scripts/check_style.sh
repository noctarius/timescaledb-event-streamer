#!/usr/bin/env bash

function join_by { local IFS="$1"; shift; echo "$*"; }


fullRun=${1}
failed=0
regex="'(func\s+(\([^)]+\))?\s?[^(]+\( ?[a-z]+.*?)|(^\) {\n[^\s]{1})'"

IFS=$'\n\r'

echo "Please wait a moment, checking style..."
if [[ "${fullRun}" == "full" ]]; then
  # Run on all files
  echo "Actually, running a full check cause you asked me to."
  files=$(find . -type f -name '*.go')
else
  # Only run for changed files
  files=$(git diff --name-only --cached)
fi
for file in ${files}
do
  # Check if go file
  if [[ "${file}" != *.go ]]; then
    continue
  fi

  # Check function signature style
  lines=$(grep -E '(func\s+(\([^)]+\))?\s?[^(]+\( ?[a-z]+.*?)|(^\) {\n[^\s]{1})' "${file}")
  linesCount=$(grep -c . <<<"${lines}")
  
  # Check lines exceeding 120 characters (tests are ignored)
  longLinesCount=0
  if [[ "${file}" != *_test.go ]]; then
    longLines=$(grep '^.\{120\}' "${file}")
    longLinesCount=$(grep -c . <<<"${longLines}")
  fi

  count=$(( ${linesCount} + ${longLinesCount} ))
  if [[ ${count} -ne 0 ]]; then
    echo "File: ${file/.\//}, found ${count} issue"

    # Print out function signature style issues
    if [[ ${linesCount} -gt 0 ]]; then
      for line in ${lines}
      do
        lineNum=$(grep -wnF "${line}" "${file}" | cut -d: -f1)
        if (( $(grep -c . <<<"${lineNum}") > 1 )); then
          echo -e "\t*lines "$(join_by , ${lineNum[@]})": ${line}"
        else
          echo -e "\t*line ${lineNum}: ${line}"
        fi
      done
    fi

    # Print out exceeding lines
    if [[ ${longLinesCount} -gt 0 ]]; then
      for line in ${longLines}
      do
        lineNum=$(grep -wnF "${line}" "${file}" | cut -d: -f1)
        if (( $(grep -c . <<<"${lineNum}") > 1 )); then
          echo -e "\t*lines "$(join_by , ${lineNum[@]})": ${line}"
        else
          echo -e "\t*line ${lineNum}: ${line}"
        fi
      done
    fi
    failed=1
  fi
done

if [ ${failed} -ne 0 ]; then
  echo "Please fix the above issues"
  exit 1
fi

echo "Running license header check..."
missingLicenseHeaders=$(license-header-checker .license.header.txt . go | grep 'files had no license but were not changed' | grep -c .)
if [[ ${missingLicenseHeaders} -ne 0 ]]; then
  echo Some files have missing license headers. Please run 'make license-header-check' for more information
  exit 1
fi

echo "Thanks for waiting, all good! 👍"
