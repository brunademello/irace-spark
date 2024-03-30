import json

def save_irace_metadata(date_ref, params: dict):
    irace_metadata = {
            "instance_id": params['instance_id'], 
            "configuration_id": params['configuration_id'], 
            "parameters": params['parameters'], 
            "begin": str(params['begin']), 
            "end": str(params['end']), 
            "total": str(params['total']), 
            "execution_status": params['execution']
        }

    file = open(f'../logs/irace-metadata-{date_ref}.txt', 'a')
    file.write(json.dumps(irace_metadata))
    file.write('\n')
    file.close()