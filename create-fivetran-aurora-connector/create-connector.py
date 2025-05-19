import requests
from requests.auth import HTTPBasicAuth
import json
import config
import os
import pandas as pd
from pandas import json_normalize
import csv
import base64


# Authenticating token from config file. 
api_key = config.api_key
api_secret = config.api_secret
password = config.password

credentials = f"{api_key}:{api_secret}"
encoded_credentials = base64.b64encode(credentials.encode()).decode()


# Set Fivetran API endpoint
api_endpoint = "https://api.fivetran.com/v1/connections" #"https://api.fivetran.com/v1/connectors"

# Create Base64 Encoded Basic Auth Header
auth = HTTPBasicAuth(api_key, api_secret)

headers = {
    "Accept": "application/json;version=2",
    "Authorization": "Basic Y05oRWRhbHBTV0Z2QllCZzpLcE16WHlLcTBQUHo5akVZTll3R0R3NEt3aTBIaDVwbQ==",
    "content-type": "application/json"
}

# transform
df = pd.read_csv(os.path.abspath(os.getcwd()) +'/bquxjob_2fac04d2_195c9cae4af.csv')
df[df.columns] = df.apply(lambda x: x.str.strip()) # trim all strings of a dataframe
df = df.replace('& ', '', regex=True).replace(',', '', regex=True). \
    replace('\.', '', regex=True).replace('’', '', regex=True).replace('\'', '_', regex=True).replace(' ', '_', regex=True)

facilities_list = df.values.tolist()


# create connector function
def create_connectors():
    
    file_path = (os.path.abspath(os.getcwd()) +'/create-connectors-log.txt')
    empty_log_file = open(file_path, "w").close()
    

    # Set the connector configuration
    for x in facilities_list:
        body = {

            "name": "Aurora Postgres", 
            "service": 'aurora_postgres',
            "group_id": "paradigm_motion",
            "trust_certificates": "true",
            "trust_fingerprints": "true",
            "run_setup_tests": "true",
            "sync_frequency": 1440,
            "daily_sync_time": "03:00",
            "paused": "False", # make this False to start the initial sync
            "run_setup_tests": "True",

            "config": {

                "schema_prefix": "src_emr_" + x[1].lower() + '_' + x[0][:8],
                "host": "heliumhealth-new-cluster-cluster.cluster-ro-cmyqyq1ypwcc.eu-west-1.rds.amazonaws.com",
                "port": "5432",
                "database": x[0],
                "user": "onemedical",
                "password": password,
                "tunnel_host": "bastion.onemedtest.com",
                "tunnel_port": 22,
                "tunnel_user": "ubuntu",
                "update_method": "XMIN"
                     },

            "state": "ENABLED",

            "type": "aurora_postgres"

               }

        # Make the API call to create the connector
        # response = requests.post(url=api_endpoint,auth=auth,json=body).json()
        response = requests.request("POST", api_endpoint, json=body, headers=headers)
        #print(response)
  
       # Generate log files
        with open(file_path, "a") as f:
            f.write(str(response))



# execute function
if __name__ == "__main__":
    create_connectors()