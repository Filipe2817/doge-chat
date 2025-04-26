#!/usr/bin/env bash
set -e

launch_node() {
  local node="$1" flags
  case "$node" in
    n1) flags="-config config/n1.config -sname n1@localhost" ;;
    n2) flags="-config config/n2.config -sname n2@localhost" ;;
    n3) flags="-config config/n3.config -sname n3@localhost" ;;
    *)
      echo "Invalid node: $node"; exit 1
      ;;
  esac

  alacritty -e bash -c \
    "export ERL_FLAGS=\"$flags\"; rebar3 shell; exec bash" &
}

for n in n1 n2 n3; do
  launch_node "$n"
done

wait
