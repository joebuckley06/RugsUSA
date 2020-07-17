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

# Column cleaners
def purchase_value(list_dict):
    try:
        return(list_dict[0]['value'])
    except:
        return(0)
    
def purchases_clean(list_dict):
    for i in list_dict:
        if i['action_type'] == 'offsite_conversion.fb_pixel_purchase':
            return(i['value'])
        else:
            continue
            
# Function to remove commas
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


my_app_id = '617482599126758'
my_app_secret = '8603285abccc490ff648f051e724830e'
my_access_token = 'EAAIxmN751uYBAMZB5DwQlbtAEVAC7zWTty2yBZC1OnBAPm4CEopjqPT2uHUwnVwXEVzKLiSl14hOtJ7fWGMYvx1ZA62E5rfZCFOZCYZBh94niKZCequ2R3SxLvrsOVUTXZBofjjLeSWWFZCU6C5ddCFzbvDlAfnaPdVKN1gpp5bwQOB9hJBsguMiE'
FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
my_account = AdAccount('act_101957673224170')

def update_AWS():
    # Download data currently in Database
    df_fb = pd.read_sql_query('''SELECT * FROM facebook_spend;''', cnx)
    
    # Get latest dates updated
    max_db_date_plus_one = str(pd.to_datetime(max(df_fb['Date'])) + timedelta(1))[:10]
    print(max_db_date_plus_one)
    yesterday = str(datetime.today() - timedelta(1))[:10]
    
    # Update if necessary
    if max_db_date_plus_one < yesterday:
        new_data = my_account.get_insights(fields=['spend','clicks','impressions','campaign_name','actions','action_values'],
                       params={'time_range':{'since':max_db_date_plus_one,'until':yesterday},
                               'level':'campaign',
                               'time_increment':'1'})
        df_fb_new = pd.DataFrame(new_data)
        df_fb_new['purchase_value'] = df_fb_new['action_values'].apply(purchase_value).astype(float)
        df_fb_new['spend'] = df_fb_new['spend'].astype(float)
        df_fb_new['purchases'] = df_fb_new['actions'].apply(purchases_clean)
        df_fb_new['Channel'] = 'Facebook'
        df_fb_clean = df_fb_new[[ 'Channel',
                       'date_start', 
                       'campaign_name', 
                       'spend', 
                       'impressions', 
                       'clicks', 
                       'purchases', 
                       'purchase_value']].copy()
        df_fb_clean = df_fb_clean.rename(columns={'date_start':'Date',
                                                  'campaign_name':'Campaign',
                                                  'spend':'Spend',
                                                  'impressions':'Impressions',
                                                  'clicks':'Clicks',
                                                  'purchases':'Conversions',
                                                  'purchase_value':'Revenue'})
        df_fb_clean['Category'] = 'Paid Social - Facebook'
        df_fb_clean['Tuesday_Week'] = df_fb_clean['Date'].apply(tuesday_week)
        df_fb_clean['Impressions'] = df_fb_clean['Impressions'].astype(int)
        df_fb_clean['Clicks'] = df_fb_clean['Clicks'].astype(int)
        df_fb_clean['Conversions'] = df_fb_clean['Conversions'].fillna(0).astype(int)

        # Append to existing table
        # sqlEngine = create_engine('mysql+pymysql://root:@127.0.0.1/test', pool_recycle=3600)
        dbConnection = cnx.connect()

        tableName = 'facebook_spend'

        try:
            frame = df_fb_clean.to_sql(tableName, dbConnection, if_exists='append');
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