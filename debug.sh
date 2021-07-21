#!/bin/bash

set -e

BASEDIR=$(dirname "$(readlink -f $0)")

banner() {
  echo -e "\n------------------------------------------------------------\n"
  echo "            _               _   _                _   _  ___ "
  echo " |/   /\   |_  |/   /\     /   / \  |\ |  |\ |  |_  /    |  "
  echo " |\  /--\  |   |\  /--\    \_  \_/  | \|  | \|  |_  \_   |  "
  echo -e "\n------------------------------------------------------------\n"
}

usage() {
  echo "Usage: $0 [options]" 1>&2 && \
  echo -e "\t -b \t: Build project and the Docker image." 1>&2 && \
  echo -e "\t -h\t: Print this Help." 1>&2; exit 1;
}

banner

BUILD="false"
while getopts "bh" o; do
    case "${o}" in
        b) BUILD="true";;
        h|*)
            usage
            ;;
    esac
done

if [[ "$BUILD" == "true" ]]; then
  echo -e "\n 🏭 Building project..."
  (cd "$BASEDIR"; make docker-build);
fi

echo -e "\n🐳 Stopping previous Kafka Docker-Compose stack..."
(cd "$BASEDIR"; docker-compose -f ./docker-compose-debug.yml down)

echo -e "\n🐳 Starting Kafka Docker-Compose stack..."
(cd "$BASEDIR"; docker-compose -f ./docker-compose-debug.yml up -d --scale connect=2)

echo -e "\n⏳ Waiting for Kafka Connect..."
CONNECT_URL=http://localhost:80/connectors
while [ $(curl -s -o /dev/null -w %{http_code} ${CONNECT_URL}) != 200 ]; do
  echo -e $(date) "\tKafka Connect HTTP state: " $(curl -k -s -o /dev/null -w %{http_code} ${CONNECT_URL}) " (waiting for 200)"
  sleep 2
done
echo -e "\n 🚀 Woohoo! Kafka Connect is up! ($CONNECT_URL)"

exit 0
