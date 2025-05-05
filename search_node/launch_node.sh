#!/usr/bin/env bash
set -e

if [ $# -ne 1 ]; then
  echo "Usage: $0 <node>"
  echo "Available nodes: n1 to n5"
  exit 1
fi

NODE="$1"
CONFIG="config/${NODE}.config"

if [ ! -f "$CONFIG" ]; then
  echo "Configuration file $CONFIG does not exist."
  exit 1
fi

FLAGS="-config ${CONFIG} -sname ${NODE}@localhost"

# Launch node
alacritty -e bash -c "export ERL_FLAGS=\"${FLAGS}\"; rebar3 shell; exec bash" &
