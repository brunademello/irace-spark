from WordCount import WordCountSparkCassandraIrace
from datetime import datetime
import json
import time
import sys

date_ref = datetime.now().date().strftime("%Y-%m-%d")

print(datetime.now(), 'Starting process')

flag_irace = False
execution = True

# iRace mode: irace chooses the parameters in bash call
if len(sys.argv) > 1 and sys.argv[1] not in ('local', 'default'):

    flag_irace = True

    configuration_id = sys.argv[2]
    instance_id = sys.argv[3]
    seed = sys.argv[4]
    instance = sys.argv[5]
    cand_params = sys.argv[6:]

    parameters = {
        "memory": f"{cand_params.pop(0)}g",
        "cores": cand_params.pop(0),      
        "shufflePartitions": cand_params.pop(0),
        "parallelism": cand_params.pop(0),
        "speculation": f"{cand_params.pop(0)}",
        "partitionOverwriteMode": f"{cand_params.pop(0)}",
        "cassandra_output_consistency_level": f"{cand_params.pop(0)}",
        "cassandra_input_split_size_in_mb": cand_params.pop(0),
        "cassandra_input_batch_size_rows": cand_params.pop(0),
        "cassandra_input_batch_grouping_buffer_size": cand_params.pop(0)
    }

    wordcount_obj = WordCountSparkCassandraIrace(parameters)

# running script in local mode with parameters decided by user
elif len(sys.argv) > 1 and sys.argv[1] == 'local':

    parameters = {
        "memory": f"{sys.argv[3]}g",
        "cores": sys.argv[4],      
        "shufflePartitions": sys.argv[5],
        "parallelism": sys.argv[6],
        "speculation": f"{sys.argv[7]}",
        "partitionOverwriteMode": f"{sys.argv[8]}",
        "cassandra_output_consistency_level": f"{sys.argv[9]}",
        "cassandra_input_split_size_in_mb": sys.argv[10],
        "cassandra_input_batch_size_rows": f"{sys.argv[11]}",
        "cassandra_input_batch_grouping_buffer_size": sys.argv[12]
    }

    wordcount_obj = WordCountSparkCassandraIrace(parameters)  

# running script with spark default configuration 
else:
    parameters = {}
    wordcount_obj = WordCountSparkCassandraIrace()


file_path = '/home/ubuntu/projeto/spark-wordcount-cassandra-main/files/logs/to_use'

for i in range(11):
    try:
            begin = datetime.now()
            wordcount_obj.logs_word_count(path=file_path)
            end = datetime.now()
            total = (end - begin).total_seconds() #/60
    except Exception as error:
            end = datetime.now()
            execution = False
            print(error)
            total = 100000000

    print(datetime.now(), 'Process finished')
    print("\n")
    print(total)

    if flag_irace:
            #wordcount_obj.irace_save_metadata(execution_id=str(sys.argv[1]), instance_id=instance_id, configuration_id=configuration_id, parameters=parameters, begin=begin, end=end, total=total, execution_status=execution)
        irace_metadata = {
                    "execution_id":str(sys.argv[1]), 
                    "instance_id":instance_id, 
                    "configuration_id": configuration_id, 
                    "parameters":parameters, 
                    "begin":str(begin), 
                    "end":str(end), 
                    "total":str(total), 
                    "execution_status":execution
            }
        file = open(f'logs/irace_metadata_{date_ref}.txt', 'a')
        file.write(json.dumps(irace_metadata))
        file.write('\n')
        file.close()

    if (len(sys.argv) > 1 and sys.argv[1] == 'local') or (len(sys.argv) == 2):
            if sys.argv[1] == 'default':
                execution_result = {
                        'config_id': sys.argv[1],
                        'total': total
                }
            else:
                execution_result = {
                        'config_id': sys.argv[2],
                        'total': total
                }
            file = open(f'metrics/config_metrics_{date_ref}.txt', 'a')
            file.write(json.dumps(execution_result))
            file.write('\n')
            file.close()

wordcount_obj.spark_stop()
