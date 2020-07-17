from __future__ import print_function
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import sqlalchemy
import psycopg2
from sqlalchemy import create_engine
import json

with open('Rugs_AWS_creds.json') as json_file:
    creds = json.load(json_file)
    
# Write all creds
host = creds['host']
port = creds['port']
username = creds['username']
password = creds['password']
database = creds['database']

# Postgres username, password, and database name
MYSQL_ADDRESS = host
## INSERT YOUR DB ADDRESS IF IT'S NOT ON PANOPLY
MYSQL_PORT = port

## CHANGE THIS TO YOUR PANOPLY/POSTGRES USERNAME
MYSQL_USERNAME = username

## CHANGE THIS TO YOUR PANOPLY/POSTGRES PASSWORD 
MYSQL_PASSWORD = password

## CHANGE THIS TO YOUR DATABASE NAME
MYSQL_DBNAME = database

# A long string that contains the necessary Postgres login information
MYSQL_str = ('mysql+pymysql://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(username=MYSQL_USERNAME, password=MYSQL_PASSWORD, ipaddress=MYSQL_ADDRESS, port=MYSQL_PORT,dbname=MYSQL_DBNAME))

# Create the connection
cnx = create_engine(MYSQL_str, pool_recycle=3600) 

# Criteo date function
def date_printer(bad_date):
    clean_date = bad_date[4:]
    new_date = str(pd.to_datetime(clean_date))[:11]
    return(new_date)

# Tuesday Week
def standard_weekstart(string_date):
    return(pd.to_datetime(string_date).weekday())

def tuesday_weekstart(string_date):
    return((pd.to_datetime(string_date).weekday() - 1) % 7)

def tuesday_week(string_date):
    date_diff = (pd.to_datetime(string_date).weekday() - 1) % 7
    return(str(pd.to_datetime(string_date)-timedelta(date_diff))[:10])


import time
import criteo_marketing
from criteo_marketing.rest import ApiException
from pprint import pprint
import requests 

# Create an instance of the API class
api_instance = criteo_marketing.AuthenticationApi()

# Criteo creds
with open('criteo_creds.json') as json_file:
    criteo_creds = json.load(json_file)
    
# Write all creds
client_id = criteo_creds['client_id']
client_secret = criteo_creds['client_secret']
grant_type = criteo_creds['grant_type']

try:
    # Authenticates provided credentials and returns an access token
    api_response = api_instance.o_auth2_token_post(client_id=client_id, client_secret=client_secret, grant_type=grant_type)
    b_token = api_response.access_token
#     pprint(api_response)
except ApiException as e:
    print("Exception when calling AuthenticationApi->o_auth2_token_post: %s\n" % e)
    
def update_AWS():
    # Pull Criteo DB data
    df_criteo = pd.read_sql_query('''SELECT * FROM criteo_spend;''', cnx)
    
    # Dates to variables
    max_db_date_plus_one = str(pd.to_datetime(max(df_criteo['Date'])) + timedelta(1))[:10]
    yesterday = str(datetime.today() - timedelta(1))[:10]
    
    # Update if necessary
    if max_db_date_plus_one <= yesterday:
        stats_endpoint = 'https://api.criteo.com/marketing/v1/statistics'
        headers = {"Authorization": "Bearer "+b_token}
        params = {
          "reportType": "CampaignPerformance",
          "advertiserIds": "6128",
          "startDate": max_db_date_plus_one,
          "endDate": yesterday,
          "dimensions": [
            "CampaignId","Day"
          ],
          "metrics": [
            "Clicks","Displays","AdvertiserCost","RevenueGeneratedPc","RevenueGeneratedPcPv","SalesAllPc","SalesPc"
          ],
          "format": "Json",
          "currency": "USD",
          "timezone": "EST"
                 }
        raw_data = requests.post(stats_endpoint, data=params, headers=headers).json()
        criteo_df = pd.DataFrame(raw_data['Rows'])
        criteo_df['Date'] = criteo_df['Day'].apply(date_printer)
        criteo_df = criteo_df.rename(columns={'Sales':'Conversions',
                                             'Cost':'Spend',
                                             'Campaign Name':'Campaign',})
        criteo_df['Channel'] = 'Criteo'
        criteo_df = criteo_df[['Channel','Date', 'Campaign','Spend',
                               'Impressions','Clicks', 'Conversions','Revenue']].copy()
        criteo_df['Category'] = 'Display Retargeting'
        criteo_df['Revenue'] = criteo_df['Revenue'].astype(float).round(2)
        criteo_df['Spend'] = criteo_df['Spend'].astype(float).round(2)
        criteo_df['Impressions'] = criteo_df['Impressions'].astype(int)
        criteo_df['Clicks'] = criteo_df['Clicks'].astype(int)
        criteo_df['Conversions'] = criteo_df['Conversions'].astype(int)
        criteo_df['Tuesday_Week'] = criteo_df['Date'].apply(tuesday_week)

        # Append to existing table
        # sqlEngine = create_engine('mysql+pymysql://root:@127.0.0.1/test', pool_recycle=3600)
        dbConnection = cnx.connect()

        tableName = 'criteo_spend'

        try:
            frame = criteo_df.to_sql(tableName, dbConnection, if_exists='append');
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:
            print("Table %s updated successfully."%tableName);   
        finally:
            dbConnection.close()
    else:
        print("Data already updated")