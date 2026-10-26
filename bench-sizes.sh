#!/usr/bin/env bash
set -u

POLICY="${1:-never}"
PORT=6380
SIZES=(5000 20000 50000 100000 200000)

for n in "${SIZES[@]}"; do
  rm -rf ./data
  LOG="/tmp/lsm-${POLICY}-n${n}.log"

  bin/lsm-server --addr ":${PORT}" --data-dir ./data --wal-sync "${POLICY}" \
    > "${LOG}" 2>&1 &
  SERVER_PID=$!

  for _ in $(seq 1 50); do
    redis-cli -p "${PORT}" PING >/dev/null 2>&1 && break
    sleep 0.1
  done

  echo "policy=${POLICY} n=${n}"
  redis-benchmark -p "${PORT}" -t set -n "${n}" -c 50 -d 100 -r 1000000 -q 2>&1 \
    | grep -v "Could not fetch server CONFIG"

  kill -TERM "${SERVER_PID}"
  wait "${SERVER_PID}" 2>/dev/null
done
