#!/bin/bash

config=$(tail -n 1 config.txt) 

read configId <<< "$config"

nohup python3 /home/bruna/irace/main.py --configId $1 --instanceId $2 --seed $3 --instance $4 ${@:5}>> logs/python_log_"$config".log 2>&1  </dev/null & 

process_id=$!

wait $process_id

tag=$( tail -n 1  logs/python_log_"$configId".log )

echo $tag
