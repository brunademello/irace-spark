import argparse
from datetime import datetime

def build_parser():

    parser = argparse.ArgumentParser()
    parser.add_argument('--instanceId', type=str, help='Valor do parâmetro speculation')
    parser.add_argument('--instance', type=str, help='Valor do parâmetro speculation')
    parser.add_argument('--configId', type=str, help='Valor do parâmetro speculation')
    parser.add_argument('--seed', type=int, help='Valor do parâmetro speculation')
    parser.add_argument('--executorMemory', type=int, help='Valor do parâmetro speculation')
    parser.add_argument('--driverCores', type=int, help='Valor do parâmetro speculation')
    parser.add_argument('--sqlShufflePartitions', type=int, help='Valor do parâmetro speculation')
    parser.add_argument('--defaultParallelism', type=int, help='Valor do parâmetro speculation')
    parser.add_argument('--memoryFraction', type=float, help='Valor do parâmetro speculation')
    parser.add_argument('--shuffleCompress', type=bool, help='Valor do parâmetro speculation')
    parser.add_argument('--partitionOverwriteMode', type=str, help='Valor do parâmetro speculation')
    parser.add_argument('--cassandraOutputConsistencyLevel', type=str, help='Valor do parâmetro speculation')
    parser.add_argument('--cassandraInputSplitSizeinMB', type=int, help='Valor do parâmetro speculation')
    parser.add_argument('--cassandraOutputBatchSizeRows', type=str, help='Valor do parâmetro speculation')
    parser.add_argument('--cassandraOutputBatchGroupingBufferSize', type=int, help='Valor do parâmetro speculation')
    parser.add_argument('--cassandraOutputConcurrentWrites', type=int, help='Valor do parâmetro speculation')

    return parser
