#!/bin/bash

INPUT_CSV=$1
OUTPUT_CSV=$2
SHARED_DIR="shared_dir"

podman run --rm -v "$(pwd)":/${SHARED_DIR} ldap-identification -input-csv="${SHARED_DIR}/${INPUT_CSV}" -output="${SHARED_DIR}/${OUTPUT_CSV}" -v 2 -log-file="${SHARED_DIR}/output.log"

