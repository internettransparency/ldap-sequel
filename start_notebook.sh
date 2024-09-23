#!/bin/bash

# See .env and docker-compose.yml
podman-compose pull
PODMAN_USERNS=keep-id podman-compose up &> session &
echo "See \"session\" file for the notebook url"
