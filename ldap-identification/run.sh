#!/usr/bin/bash

INPUT_CSV=$1
OUTPUT_CSV=$2

podman run --rm -v "$(pwd)":/ ldap-identification -input-csv="${INPUT_CSV}" -output="${OUTPUT_CSV}" -v 2 -log-file="output.log"
