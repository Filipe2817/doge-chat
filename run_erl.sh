#!/bin/bash

# Debugging
#set -x

set -eu

YELLOW=$(tput setaf 3)
NORMAL=$(tput sgr0)

msg() {
  printf "%s%s%s\n" "$YELLOW" "$1" "$NORMAL"
}

compile() {
  if [ ! -d ebin ]; then
    msg "Creating ebin directory..."
    mkdir ebin
  fi

  msg "Compiling..."

  if ! erlc -o ebin search_server/*.erl 
  then
    msg "Compilation failed."
    exit 1
  fi
}

check_processes() { # To kill every process: pgrep erl | xargs kill -9
  local server_pid tree child_pids
  server_pid=$(pgrep -f "search_server")
  tree=$(pstree -p | grep "beam.smp($server_pid)")
  child_pids=$(echo "$tree" | grep -oP '\(\d+\)' | tr -d '()' | grep -v "^$server_pid$")
  
  if [ -n "$server_pid" ] || [ -n "$child_pids" ]; then
    msg "Server is still running with PID: $server_pid"
    msg "Children PIDs: [$child_pids]"
  fi
}

MODULE="search_server"
NODE_NAME="server_node"

start() {
  msg "Starting server..."
  erl -pa ebin \
      -s "${MODULE}" \
      -sname "${NODE_NAME}" \
      -setcookie sdoge \
      -noshell -noinput > server.log 2>&1 &
  msg "OK"
}

stop() {
  msg "Stoping server..."
  erl -sname stop_node \
      -setcookie sdoge \
      -eval "rpc:call(${NODE_NAME}@$(uname -n), ${MODULE}, stop, []), init:stop()." \
      -noshell -noinput
  msg "OK"
}

help() {
  msg "Usage: $0 <command>"
  msg "  - start    : Start Search Server"
  msg "  - stop     : Stop Search Server"
  msg "  - help     : Display this help message"
}

case "${1:-help}" in
  "start")
    compile
    start
    ;;
  "stop")
    stop
    check_processes
    ;;
  "help"|*)
    help
    ;;
esac
