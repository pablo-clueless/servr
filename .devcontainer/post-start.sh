#!/usr/bin/env bash
# Brings the data plane up when the dev container starts.
#
# The docker-in-docker daemon starts in parallel with this script, so a bare
# `docker compose up` here races it and fails on a cold start roughly one time
# in three. That failure looks exactly like broken infra, which is the most
# expensive kind of confusing.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "waiting for the docker daemon..."
for _ in $(seq 1 60); do
  if docker info >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! docker info >/dev/null 2>&1; then
  echo "docker daemon did not come up; run 'make up' once it has" >&2
  exit 0 # Not fatal: the container is still usable for building and unit tests.
fi

# `--wait` blocks on the healthchecks rather than merely on container start.
# That is trap T2: without it the server races infra and the failure surfaces
# later as a flaky test.
echo "starting postgres, redis, mailpit..."
docker compose up -d --wait

echo
docker compose ps --format '{{.Service}}\t{{.Health}}'
echo
echo "data plane ready. 'make gate-0' verifies it; 'make up-obs' adds jaeger + prometheus."
