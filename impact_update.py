from __future__ import print_function
import pandas as pd
import numpy as np
import requests
import os
from datetime import datetime, timedelta
import sqlalchemy
import psycopg2
from sqlalchemy import create_engine
import json
import xmltodict
import pprint
import numpy as np
from multiprocessing.dummy import Pool as ThreadPool
import time

# Impact creds
with open('impact_creds.json') as json_file:
    impact_creds = json.load(json_file)
    
# Write all creds
AccountSid = impact_creds['AccountSid']
readonly_Sid = impact_creds['readonly_Sid']
token = impact_creds['token']


# AWS creds
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

# Tuesday week function
def tuesday_week(string_date):
    date_diff = (pd.to_datetime(string_date).weekday() - 1) % 7
    return(str(pd.to_datetime(string_date)-timedelta(date_diff))[:10])

# Impact API call functions
def update_AWS():
    df_impact_db = pd.read_sql_query('''SELECT * FROM impact_spend;''', cnx)
    max_db_date_plus_one = str(pd.to_datetime(max(df_impact_db['Date'])) + timedelta(1))[:10]
    print(max_db_date_plus_one)
    yesterday = str(datetime.today() - timedelta(1))[:10]
    
    # Run function
    if max_db_date_plus_one < yesterday:
        start_date = max_db_date_plus_one + 'T00:00:00Z'
        end_date = yesterday + 'T00:00:00Z'
        print(start_date + " to " + end_date)
        params = {"ActionDateStart":start_date,
                     "ActionDateEND": end_date,
                     "CampaignId":'9280',
                     "PageSize":'2000'}
        pages = xmltodict.parse(requests.get('https://'+AccountSid+':'+token+'@api.impact.com/Advertisers/'+AccountSid+'/Actions',params=params).content)['ImpactRadiusResponse']['Actions']['@numpages']
        print(pages)
        page_list = np.arange(1,int(pages)+1)

        def impact_api_call(n):
            page_params = {"ActionDateStart":start_date,
                     "ActionDateEND": end_date,
                     "CampaignId":'9280',
                     "PageSize":'2000',
                     "Page":str(n)}
            page_response = requests.get('https://'+AccountSid+':'+token+'@api.impact.com/Advertisers/'+AccountSid+'/Actions',params=page_params)
            actions_dict = xmltodict.parse(page_response.content)
            if page_response.status_code==200:
                df_actions = pd.DataFrame(actions_dict['ImpactRadiusResponse']['Actions']['Action'])
                df_actions_sub = df_actions[['MediaPartnerId', 'MediaPartnerName',
                    'ClientCost', 'Payout', 'DeltaPayout',
                   'IntendedPayout', 'Amount', 'DeltaAmount', 'IntendedAmount', 'Currency',
                   'ReferringDate', 'EventDate', 'CreationDate','ReferringDomain',
                   'PromoCode', 'CustomerCity', 'CustomerRegion']].sort_values('EventDate').copy()
                return(df_actions_sub)
            else:
                print(str(n)+": Fail "+ str(page_response.status_code))
                time.sleep(10)
                while page_response.status_code != 200:
                    page_response = requests.get('https://'+AccountSid+':'+token+'@api.impact.com/Advertisers/'+AccountSid+'/Actions',params=page_params)
                    actions_dict = xmltodict.parse(page_response.content)
                    time.sleep(5)
                try:
                    df_actions = pd.DataFrame(actions_dict['ImpactRadiusResponse']['Actions']['Action'])
                    df_actions_sub = df_actions[['MediaPartnerId', 'MediaPartnerName',
                        'ClientCost', 'Payout', 'DeltaPayout',
                       'IntendedPayout', 'Amount', 'DeltaAmount', 'IntendedAmount', 'Currency',
                       'ReferringDate', 'EventDate', 'CreationDate','ReferringDomain',
                       'PromoCode', 'CustomerCity', 'CustomerRegion']].sort_values('EventDate').copy()
                    print(str(n)+": Success")
                    return(df_actions_sub)
                except:
                    print(str(n)+": Fail, again")
                    pass

        # function to be mapped over
        def calculateParallel_impact(numbers, threads=2):
            pool = ThreadPool(threads)
            results = pool.map(impact_api_call, numbers)
            pool.close()
            pool.join()
            return(results)

        numbers = list(page_list)
        results = calculateParallel_impact(numbers, 6)
        df_combined = pd.concat(results)
        df_combined['Date'] = [x[:10] for x in df_combined['EventDate']]
        df_combined['Payout'] = df_combined['Payout'].astype(float)
        df_combined = df_combined.reset_index().drop('index',1)

        df_cleaner = df_combined[['MediaPartnerName','Payout','Amount','EventDate','CustomerRegion','Date']].copy()
        df_impact_rad = df_cleaner.groupby(['Date','MediaPartnerName'],as_index=False)[['Payout','Amount']].apply(lambda x : x.astype(float).sum())
        df_impact_rad = df_impact_rad.reset_index()
        df_impact_rad = df_impact_rad.rename(columns={'Payout':'Spend',
                                                      'Amount':'Revenue',
                                                     'MediaPartnerName':'Campaign'})
        df_impact_rad['Channel'] = 'Impact'
        df_impact_rad['Category'] = 'Affiliate'
        df_impact_rad['Tuesday_Week'] = df_impact_rad['Date'].apply(tuesday_week)

        # Append to existing table
        # sqlEngine = create_engine('mysql+pymysql://root:@127.0.0.1/test', pool_recycle=3600)
        dbConnection = cnx.connect()

        tableName = 'impact_spend'

        try:
            frame = df_impact_rad.to_sql(tableName, dbConnection, if_exists='append');
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