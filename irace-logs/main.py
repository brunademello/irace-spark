from WordCount import WordCountSparkCassandraIrace
from helpers.parser import build_parser
from helpers.irace_metadata import save_irace_metadata
from datetime import datetime, timedelta
import sys

bash_parameters = sys.argv

args = build_parser().parse_args()

date_ref = (datetime.now() - timedelta(hours=3)).date().strftime("%Y-%m-%d")

print(datetime.now(), 'Starting process')

configuration_id = args.configId
instance_id = args.instanceId
seed = args.seed
instance = args.instance

file_path = f'/home/bruna/irace/files/{instance}'

parameters = {}

for key, value in vars(args).items():
    if value != None:
        if key == 'executorMemory':
            parameters[key] = str(value) + 'G'
        else:
            parameters[key] = value

wordcount_obj = WordCountSparkCassandraIrace(parameters)

try:
    begin = datetime.now()
    wordcount_obj.logs_word_count(path=file_path)
    end = datetime.now()
    total = (end - begin).total_seconds() 
        
    execution = True

except Exception as error:
    end = datetime.now()
    execution = False
    print(error)
    # adding an arbitrary value high enough to not impact the final result 
    # probably this confguration will be discarded when Irace is running
    total = 100000000


# Building dict with irace metadata to save every execution of this script
    
if instance_id is not None:
    # if instance_id is not None it means that Irace is running
    # because this value is definied by Irace

    irace_metadata = {
        'instance_id': instance_id,
        'configuration_id': configuration_id,
        'parameters': parameters,
        'begin': begin, 
        'end': end,
        'total': total,
        'execution_status': execution
    }

    save_irace_metadata(date_ref, irace_metadata)

# DO NOT REMOVE THIS PRINT AND DO NOT PRINT ANYTHING ELSE AFTER THIS
# THE VALUE WILL BE USE BY IRACE
print(total)

wordcount_obj.spark_stop()
