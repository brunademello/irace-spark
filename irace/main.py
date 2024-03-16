from WordCount import WordCountSparkCassandraIrace
from datetime import datetime, timedelta
import json
import sys
import argparse

bash_parameters = sys.argv

parser = argparse.ArgumentParser()
parser.add_argument('--instanceId', type=int, help='Valor do parâmetro speculation')
parser.add_argument('--instance', type=int, help='Valor do parâmetro speculation')
parser.add_argument('--configId', type=int, help='Valor do parâmetro speculation')
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
parser.add_argument('--cassandraInputBatchSizeRows', type=str, help='Valor do parâmetro speculation')
parser.add_argument('--cassandraInputBatchGroupingBufferSize', type=int, help='Valor do parâmetro speculation')

args = parser.parse_args()

date_ref = (datetime.now() -  timedelta(hours=3)).date().strftime("%Y-%m-%d")

print(datetime.now(), 'Starting process')

flag_irace = True

file_path = '/home/bruna/irace/files'

all_parameters = ["executorMemory", 
                    "driverCores", 
                    "sqlShufflePartitions", 
                    "defaultParallelism", 
                    "memoryFraction", 
                    "shuffleCompress",
                    "partitionOverwriteMode", 
                    "cassandraOutputConsistencyLevel", 
                    "cassandraInputSplitSizeinMB", 
                    "cassandraInputBatchSizeRows", 
                    "cassandraInputBatchGroupingBufferSize"]

configuration_id = args.configId
instance_id = args.instanceId
seed = args.seed
instance = args.instance

parameters = {}

for parameter in all_parameters:
    if parameter == 'executorMemory':
        parameters[parameter] = str(getattr(args, parameter)) + 'G'
    else:
        parameters[parameter] = getattr(args, parameter)

wordcount_obj = WordCountSparkCassandraIrace(parameters)

try:
    begin = datetime.now()
    wordcount_obj.logs_word_count(path=file_path)
    total_rows = wordcount_obj.read_and_filter_data()
    print("Total de IPs filtrados:", total_rows)
    end = datetime.now()
    total = (end - begin).total_seconds() 
except Exception as error:
    end = datetime.now()
    execution = False
    print(error)
    total = 100000000

irace_metadata = {
        "execution_id":str(bash_parameters[1]), 
        "instance_id":instance_id, 
        "configuration_id": configuration_id, 
        "parameters":parameters, 
        "begin":str(begin), 
        "end":str(end), 
        "total":str(total), 
        "execution_status":execution
    }
file = open(f'logs/irace-metadata-{date_ref}.txt', 'a')
file.write(json.dumps(irace_metadata))
file.write('\n')
file.close()


wordcount_obj.spark_stop()
