#!/bin/bash

python3 /home/ubuntu/projeto/spark-wordcount-cassandra-main/main.py default

python3 /home/ubuntu/projeto/spark-wordcount-cassandra-main/main.py local config1 5 1 500 1 true dynamic ANY 128 100 3000

python3 /home/ubuntu/projeto/spark-wordcount-cassandra-main/main.py local config2 10 1 500 1 true static ANY 128 100 5000

python3 /home/ubuntu/projeto/spark-wordcount-cassandra-main/main.py local config3 15 1 500 1 true dynamic ANY 128 100 3000

python3 /home/ubuntu/projeto/spark-wordcount-cassandra-main/main.py local config4 17 6 200 1 true dynamic ANY 32 500 4000

python3 /home/ubuntu/projeto/spark-wordcount-cassandra-main/main.py local config5 26 5 500 3 true dynamic ANY 64 100 2000
