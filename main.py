from WordCount import WordCountSparkCassandraIrace
from cassandra.cluster import Cluster
from datetime import datetime
import sys

print(datetime.now(), 'Starting process')

cluster = Cluster(['localhost'])
session = cluster.connect()

# removing data from logs table every execution 
#session.execute("TRUNCATE TABLE analytics_data.logs")
#session.execute("TRUNCATE TABLE analytics_data.logs_agg")
session.execute("TRUNCATE TABLE analytics_data.logs_wordcount")

flag_irace = False
execution = True

if len(sys.argv) > 1 and sys.argv[1] != 'local':

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

elif len(sys.argv) > 1 and sys.argv[1] == 'local':

    parameters = {
        "memory": f"{sys.argv[2]}g",
        "cores": sys.argv[3],      
        "shufflePartitions": sys.argv[4],
        "parallelism": sys.argv[5],
        "speculation": f"{sys.argv[6]}",
        "partitionOverwriteMode": f"{sys.argv[7]}",
        "cassandra_output_consistency_level": f"{sys.argv[8]}",
        "cassandra_input_split_size_in_mb": sys.argv[9],
        "cassandra_input_batch_size_rows": sys.argv[10],
        "cassandra_input_batch_grouping_buffer_size": sys.argv[11]
    }

    wordcount_obj = WordCountSparkCassandraIrace(parameters)    

else:
    parameters = {}
    wordcount_obj = WordCountSparkCassandraIrace()


file_path = '/home/ubuntu/irace-wordcount/files/logs'

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
    wordcount_obj.irace_save_metadata(execution_id=str(sys.argv[1]), instance_id=instance_id, configuration_id=configuration_id, parameters=parameters, begin=begin, end=end, total=total, execution_status=execution)

wordcount_obj.spark_stop()
