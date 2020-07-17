from __future__ import print_function
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import sqlalchemy
import psycopg2
from sqlalchemy import create_engine
import json
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

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


def remove_comma(num_string):
    try:
        if ',' in num_string:
            return(num_string.replace(',',''))
        else:
            return(num_string)
    except:
        return(num_string)
    
# Tuesday week function
def tuesday_week(string_date):
    date_diff = (pd.to_datetime(string_date).weekday() - 1) % 7
    return(str(pd.to_datetime(string_date)-timedelta(date_diff))[:10])

def update_AWS_pin(pd_pinterest):
    # Download data currently in Database
    df_pinterest = pd.read_sql_query('''SELECT * FROM pinterest_spend;''', cnx)
    
    # Get latest dates updated
    max_db_date_plus_one = str(pd.to_datetime(max(df_pinterest['Date'])) + timedelta(1))[:10]
    print(max_db_date_plus_one)
    yesterday = str(datetime.today() - timedelta(1))[:10]
    
    
    
    if max_db_date_plus_one < yesterday:
        # Edit table
        max_date_new = max(pd_pinterest['Date'])
        pd_pinterest['Spend in account currency'] = pd_pinterest['Spend in account currency'].apply(remove_comma).astype(float)
        pd_pinterest['Impressions'] = pd_pinterest['Paid impressions'].apply(remove_comma).astype(float)
        pd_pinterest['Clicks'] = pd_pinterest['Paid link clicks'].apply(remove_comma).astype(float)
        pd_pinterest['Conversions'] = pd_pinterest['Conversions (Checkout)'].apply(remove_comma).astype(float)
        pd_pinterest['Order value (Checkout)'] = pd_pinterest['Order value (Checkout)'].apply(remove_comma).astype(float)
        pd_pinterest_clean = pd_pinterest.drop('Campaign status',1)
        pd_pinterest_clean = pd_pinterest_clean.drop('Campaign ID',1)
        pd_pinterest_clean = pd_pinterest_clean.drop('Paid impressions',1)
        pd_pinterest_clean = pd_pinterest_clean.drop('Paid link clicks',1)
        pd_pinterest_clean = pd_pinterest_clean.drop('Conversions (Checkout)',1)
        pd_pinterest_clean = pd_pinterest_clean.rename(columns={'Campaign name':'Campaign',
                                                                'Spend in account currency':'Spend',
                                                                'Link clicks':'Clicks',
                                                                'Order value (Checkout)':'Revenue'})
        pd_pinterest_clean['Tuesday_Week'] = pd_pinterest_clean['Date'].apply(tuesday_week)
        pd_pinterest_clean['Category'] = 'Paid Social - Pinterest'
        # Make sure dates don't overlap
        df_pin_add = pd_pinterest_clean[pd_pinterest_clean['Date']>=max_db_date_plus_one].copy()
        df_pin_add = df_pin_add[df_pin_add['Date']<max_date_new].copy()
        
        # Append to existing table
        # sqlEngine = create_engine('mysql+pymysql://root:@127.0.0.1/test', pool_recycle=3600)
        dbConnection = cnx.connect()

        tableName = 'pinterest_spend'

        try:
            frame = df_pin_add.to_sql(tableName, dbConnection, if_exists='append');
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