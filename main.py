import pandas as pd
import json
import argparse
import data
parser = argparse.ArgumentParser()
parser.add_argument('--data', type=str, required=True)
args = parser.parse_args()


df=pd.read_csv('output/output.csv')
print(json.dumps(json.loads(df.to_json(orient='index')), indent=2))