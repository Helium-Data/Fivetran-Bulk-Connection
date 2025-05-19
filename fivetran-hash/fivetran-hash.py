import requests
import os
import pandas as pd
from requests.auth import HTTPBasicAuth
import config


# Authenticating token from config file. 
api_key = config.api_key
api_secret = config.api_secret
password = config.password


# Create Base64 Encoded Basic Auth Header
auth = HTTPBasicAuth(api_key, api_secret)



df = pd.read_csv(os.path.abspath(os.getcwd()) +'/connectors_schema.csv')
df[df.columns] = df.apply(lambda x: x.str.strip()) # trim all strings of a dataframe

facilities_list = df.values.tolist()
facilities_list



def update_schema():

    for x in facilities_list:
    # Set Fivetran API endpoint
        url = "https://api.fivetran.com/v1/connectors/{}/schemas/{}/tables/{}".format(x[0],x[2],x[3])

    
        body = {
                "enabled": True,
                "columns": {
                    x[4]: {
                        "hashed": True
                    }
                    }
            }


    # Make the API call to update schema
        response = requests.patch(url=url,auth=auth,json=body).json()
        # print(response)
        print(x[1],x[3],x[4],'has beem hashed successfully......')
        
        

def historical_sync():

    for x in facilities_list:
        connector_name = x[1]
        schema_name = x[2]
        table_name = x[3]
        column_name = x[4]
    # Set Fivetran API endpoint
        url = "https://api.fivetran.com/v1/connectors/{}/resync".format(x[0])

        body = {
                "scope": {
                schema_name: [table_name] 
                }
            }

        # Make the API call to sync historical data
        response = requests.post(url=url,auth=auth,json=body).json()
        # print(response)
        print(connector_name,table_name,column_name,'has beem sync successfully......')


# execute function
if __name__ == "__main__":
    update_schema()
    historical_sync()