from WordCount import WordCountSparkCassandraIrace
from datetime import datetime
import sys

flag_irace = False
execution = True

if len(sys.argv) > 1:

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

else:
    parameters = {}
    wordcount_obj = WordCountSparkCassandraIrace()


file_path = '/home/ubuntu/irace-wordcount/files'

try:
    begin = datetime.now()

    wordcount_obj.word_count(path=file_path)

    end = datetime.now()
    total = (end - begin).total_seconds()/60

except Exception as error:
    end = datetime.now()
    execution = False
    print(error)
    total = 100000000

print("\n")
print(total)

if flag_irace:
    wordcount_obj.irace_save_metadata(execution_id=sys.argv[1], instance_id=instance_id, configuration_id=configuration_id, parameters=parameters, begin=begin, end=end, total=total, execution_status=execution)

wordcount_obj.spark_stop()
