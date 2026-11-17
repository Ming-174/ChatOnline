#!/bin/bash

CLIENTS=250
RUNNING_TIME=60

pids=()

for((i=0;i<CLIENTS;i++))
do

./cli_sd "cli_${i}" &
pid=$!

pids+=("${pid}")

done

sleep "$RUNNING_TIME"

for pid in "${pids[@]}"
do

kill -INT "$pid"

done

for pid in "${pids[@]}"
do

wait "$pid"

done