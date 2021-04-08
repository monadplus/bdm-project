#!/bin/bash -e

IMAGE_NAME="dajobe/hbase"
CONTAINER_NAME="hbase-docker"

program=$(basename "$0")

echo "$program: Stopping HBase container"
data_dir="$PWD/data"
rm -rf "$data_dir"
mkdir -p "$data_dir"

# force kill any existing container
docker rm -f "${CONTAINER_NAME}" >/dev/null
