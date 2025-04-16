#!/bin/bash
maelstrom test -w lin-kv --bin ./lin_kv.erl --node-count 1 --concurrency 2 --time-limit 10
