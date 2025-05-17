#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <node>"
  echo "Available nodes: n1 to n5"
  exit 1
fi

NODE="$1"
CONFIG_FILE="config/${NODE}.config"

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Error: configuration file '$CONFIG_FILE' does not exist." >&2
  exit 1
fi

export ERL_FLAGS="-config ${CONFIG_FILE} -sname ${NODE}@localhost"

exec rebar3 shell
