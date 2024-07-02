#!/bin/bash

INPUT_CSV=$1
OUTPUT_CSV=$2
SHARED_DIR="shared_dir"

cp "${INPUT_CSV}" . 2>/dev/null
output_filename=$(basename "${OUTPUT_CSV}")
input_filename=$(basename "${INPUT_CSV}")

podman run --rm -v "$(pwd)":/${SHARED_DIR} ldap-identification -input-csv="${SHARED_DIR}/${input_filename}" -output="${SHARED_DIR}/${output_filename}" -v 2 -log-file="${SHARED_DIR}/output.log"

a=$(realpath "${input_filename}")
b=$(realpath "${INPUT_CSV}")
if [[ "${a}" != "${b}" ]]; then
    rm "${input_filename}"
fi
mv "${output_filename}" "${OUTPUT_CSV}" 2>/dev/null

