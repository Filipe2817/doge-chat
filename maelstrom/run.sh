#!/bin/bash
maelstrom test -w lin-kv --bin ./lin_kv.erl --node-count 4 --concurrency 2n --time-limit 15
