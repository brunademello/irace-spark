#!/bin/bash

output_file="test_results.txt"

# Template

#echo "Configuration X" > "$output_file"
#for ((i=1; i<=10; i++))
#do
#    python main.py --instance test_1  >> temp_output.txt
#    tail -n 1 temp_output.txt >> "$output_file"
#done

echo "Default" > "$output_file"
for ((i=1; i<=10; i++))
do
    python3 main.py --instance test_1 >> temp_output.txt 
    tail -n 1 temp_output.txt >> "$output_file"
done

echo "Configuration 1" >> "$output_file"
for ((i=1; i<=10; i++))
do
    python3 main.py --instance test_1 --executorMemory 27 --driverCores 7 --sqlShufflePartitions 300 --defaultParallelism 1 --memoryFraction 0.0719 --shuffleCompress false --partitionOverwriteMode static --cassandraOutputConsistencyLevel LOCAL_ONE --cassandraInputSplitSizeinMB 16 --cassandraOutputBatchSizeRows 3000 --cassandraOutputBatchGroupingBufferSize 3000 --cassandraOutputConcurrentWrites 20 >> temp_output.txt 
    tail -n 1 temp_output.txt >> "$output_file"
done

echo "Configuration 2" >> "$output_file"
for ((i=1; i<=10; i++))
do
    python3 main.py --instance test_1 --executorMemory 26 --driverCores 4 --sqlShufflePartitions 200 --defaultParallelism 1 --memoryFraction 0.741 --shuffleCompress true --partitionOverwriteMode static --cassandraOutputConsistencyLevel ANY --cassandraInputSplitSizeinMB 64 --cassandraOutputBatchSizeRows 2000 --cassandraOutputBatchGroupingBufferSize 3000 --cassandraOutputConcurrentWrites 20 >> temp_output.txt 
    tail -n 1 temp_output.txt >> "$output_file"
done

echo "Configuration 3" >> "$output_file"
for ((i=1; i<=10; i++))
do
    python3 main.py --instance test_1 --executorMemory 24 --driverCores 3 --sqlShufflePartitions 500 --defaultParallelism 1 --memoryFraction 0.6197 --shuffleCompress true --partitionOverwriteMode dynamic --cassandraOutputConsistencyLevel ANY --cassandraInputSplitSizeinMB 2056 --cassandraOutputBatchSizeRows 1000 --cassandraOutputBatchGroupingBufferSize 2000 --cassandraOutputConcurrentWrites 20 >> temp_output.txt 
    tail -n 1 temp_output.txt >> "$output_file"
done

echo "Configuration 4" >> "$output_file"
for ((i=1; i<=10; i++))
do
    python3 main.py --instance test_1 --executorMemory 18 --driverCores 5 --sqlShufflePartitions 200 --defaultParallelism 1 --memoryFraction 0.4798 --shuffleCompress true --partitionOverwriteMode static --cassandraOutputConsistencyLevel LOCAL_ONE --cassandraInputSplitSizeinMB 64 --cassandraOutputBatchSizeRows 3000 --cassandraOutputBatchGroupingBufferSize 3000 --cassandraOutputConcurrentWrites 20 >> temp_output.txt 
    tail -n 1 temp_output.txt >> "$output_file"
done

echo "Configuration 5" >> "$output_file"
for ((i=1; i<=10; i++))
do
    python3 main.py --instance test_1 --executorMemory 20 --driverCores 3 --sqlShufflePartitions 200 --defaultParallelism 1 --memoryFraction 0.6188 --shuffleCompress true --partitionOverwriteMode static --cassandraOutputConsistencyLevel ANY --cassandraInputSplitSizeinMB 64 --cassandraOutputBatchSizeRows 2000 --cassandraOutputBatchGroupingBufferSize 3000 --cassandraOutputConcurrentWrites 20 >> temp_output.txt
    tail -n 1 temp_output.txt >> "$output_file"
done

