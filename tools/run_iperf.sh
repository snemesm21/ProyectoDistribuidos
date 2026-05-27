#!/bin/bash
# Usage: run_iperf.sh server|client <server_ip> <duration_seconds>
if [ "$1" = "server" ]; then
  echo "Starting iperf3 server on port 5201"
  iperf3 -s
elif [ "$1" = "client" ]; then
  SERVER=$2
  DURATION=${3:-30}
  echo "Running iperf3 client to $SERVER for $DURATION seconds"
  iperf3 -c $SERVER -t $DURATION -P 4
else
  echo "Usage: $0 server|client <server_ip> [duration_seconds]"
fi
