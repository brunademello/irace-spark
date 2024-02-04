#!/bin/bash

config=$(tail -n 1 config.txt)

nohup python3 /home/ubuntu/projeto/spark-wordcount-cassandra-main/main.py $config $1 $2 $3 $4 $5 $6 $7 $8 $9 ${10} ${11} ${12} ${13} ${14}>> logs/python_log_"$config".log 2>&1  </dev/null & 

process_id=$!

wait $process_id

tag=$( tail -n 1  logs/python_log_"$config".log )

echo $tag