#!/bin/bash

# See .env and notebook-compose.yml
podman-compose pull
PODMAN_USERNS=keep-id podman-compose up &> session &
sleep 5
tail session
echo "See \"session\" file for the notebook url"
