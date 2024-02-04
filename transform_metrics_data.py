import pandas as pd 

df = pd.read_json('test_parameters.txt', orient='records', lines=True)

df.to_csv('test_parameters.csv', index=False)