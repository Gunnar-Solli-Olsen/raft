#!/bin/bash

scenario=0
num_servers=5
output_id=$(date "+%Y%m%d-%H%M%S")

addresses=()

for i in $(seq 1 $num_servers)
do
    number=$((RANDOM % (65535 - 49152 + 1) + 49152))
    address="localhost:$number"
    addresses+=("$address")
done

for address in "${addresses[@]}"
do
    echo python3 log-server.py $output_id $address "${addresses[@]}" &
    python3 log-server.py $output_id $address "${addresses[@]}" &
done

sleep 2

echo python3 log-client.py $output_id $scenario "${addresses[@]}"
python3 log-client.py $output_id $scenario "${addresses[@]}"