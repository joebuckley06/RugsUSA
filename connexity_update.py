import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import sqlalchemy
import psycopg2
from sqlalchemy import create_engine
import json
from apiclient.discovery import build
from google.oauth2 import service_account

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

# Rugs USA - GA
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'service_client_secrets.json'
VIEW_ID = '26527893'


def initialize_analyticsreporting():
    """Initializes an Analytics Reporting API V4 service object.

    Returns:
    An authorized Analytics Reporting API V4 service object.
    """
    credentials = service_account.Credentials.from_service_account_file(KEY_FILE_LOCATION)

    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    return(analytics)


def get_report(analytics,start_date='14daysAgo',end_date='today',
               metric_list = ['sessions','users','pageviews'],
               dim_list=['deviceCategory','date'],page_token=''):
    """Queries the Analytics Reporting API V4.

    Args:
    analytics: An authorized Analytics Reporting API V4 service object.
    Returns:
    The Analytics Reporting API V4 response.
    """
    # format metrics for API call
    metric_list_dict = []
    for metric in metric_list:
        metric_list_dict.append({'expression':'ga:'+metric})
        
    # format dimensions for API call
    dim_list_dict = []
    for dimension in dim_list:
        dim_list_dict.append({'name':'ga:'+dimension})
    
    
    # Make API call
    return(analytics.reports().batchGet(
        body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
          'metrics': metric_list_dict,
          'dimensions': dim_list_dict,
          'pageToken': page_token,
          'pageSize':10000
        }]
      }
    ).execute())


def print_response(response):
    """Parses and prints the Analytics Reporting API V4 response.

    Args:
    response: An Analytics Reporting API V4 response.
    """
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

    for row in report.get('data', {}).get('rows', []):
        dimensions = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])

        for header, dimension in zip(dimensionHeaders, dimensions):
            print(header + ': ' + dimension)

        for i, values in enumerate(dateRangeValues):
            print('Date range: ' + str(i))
            for metricHeader, value in zip(metricHeaders, values.get('values')):
                print(metricHeader.get('name') + ': ' + value)


def main():
    analytics = initialize_analyticsreporting()
    response = get_report(analytics)
    print_response(response)

# if __name__ == '__main__':
#     main()

def data_to_DataFrame(response):
    """
    Returns a dataframe after getting a GA data response
    """
    for report in response.get('reports', []):
            columnHeader = report.get('columnHeader', {})
            dimensionHeaders = columnHeader.get('dimensions', [])
            metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

    metric_list = [x['name'] for x in metricHeaders]

    x_report = response['reports'][0]['data']['rows']
    x_metric = list(enumerate(metric_list))
    x_dimension = list(enumerate(dimensionHeaders))


    col_dict = {}
    for dimension in x_dimension:
        col_dict[dimension[1]] = []
    for metric in x_metric:
        col_dict[metric[1]] = []

    for i in x_report:
        for dimension in x_dimension:
            col_dict[dimension[1]].append(i['dimensions'][dimension[0]])
        for metric in x_metric:
            col_dict[metric[1]].append(i['metrics'][0]['values'][metric[0]])

    GA_df = pd.DataFrame(col_dict)
    return(GA_df)

def GA_api_call(start_date='2020-01-01',end_date='2020-01-30',
                metric_list=['sessions','users','pageviews','bounces'],
                dim_list=['date']):

    analytics = initialize_analyticsreporting()
    response = get_report(analytics,start_date,end_date,metric_list=metric_list,dim_list=dim_list,page_token='')
    
    blank_dfs=[]
    
    if'nextPageToken' in list(response['reports'][0].keys()):
        print("Multiple pages of data.")
        token_list = ['']
        token = response['reports'][0]['nextPageToken']
        token_list.append(token)
        print(token)
        blank_dfs.append(data_to_DataFrame(response))
        while token:
            response_new = get_report(analytics,start_date,end_date,metric_list=metric_list,dim_list=dim_list,page_token=token)
            if'nextPageToken' in list(response_new['reports'][0].keys()):
                blank_dfs.append(data_to_DataFrame(response_new))
                token=response_new['reports'][0]['nextPageToken']
                token_list.append(token)
                print(token)
            else:
                blank_dfs.append(data_to_DataFrame(response_new))
                token=False
        print(token_list)
        return(pd.concat(blank_dfs))
    else:
        print("One page of data.")
        return(data_to_DataFrame(response))
    
def convert_date(str_date):
    year = str_date[:4]
    month = str_date[4:6]
    day = str_date[6:]
    return(pd.to_datetime(str(year)+'-'+str(month)+'-'+str(day)))

def clean_channel(lower_case_channel):
    if lower_case_channel == 'ebay_comm_net':
        return('Ebay')
    elif lower_case_channel == 'connexity':
        return('Connexity')
    elif lower_case_channel == 'google':
        return('Google')
    else:
        return(lower_case_channel)
    
def google_categories(campaign):
    if "(ROI) Dynamic" in campaign:
        return("Display Retargeting")
    elif "Discovery Ads" in campaign:
        return("CPC - Test Partners")
    elif "(ROI) Display" in campaign:
        return("Display Retargeting")
    elif "adwords_display" in campaign:
        return("Display Retargeting")
    elif "adwords_gmail" in campaign:
        return("Display Retargeting")
    elif "retargeting" in campaign:
        return("Display Retargeting")
    elif ("adwords_pla_us" in campaign) and ("brand" in campaign):
        return("SEM - PLA - Brand")
    elif "SC Shopping - TM - RugsUSA" in campaign:
        return("SEM - PLA - Brand")
    elif "adwords_pla" in campaign:
        return("SEM - PLA - Product")
    elif ("domination" in campaign) or ("GSN" in campaign) or ("RLSA GSN" in campaign) or ("Shopping" in campaign):
        return("SEM - PLA - Product")
    elif "adwords_sem_brand" in campaign:
        return("SEM - Text - Brand")
    elif "adwords_sem" in campaign:
        return("SEM - Text - Product")
    elif "(ROI)" in campaign:
        return("SEM - Text - Product")
    else:
        return("None")
    
# Tuesday week function
def tuesday_week(string_date):
    date_diff = (pd.to_datetime(string_date).weekday() - 1) % 7
    return(str(pd.to_datetime(string_date)-timedelta(date_diff))[:10])


def update_AWS():
    df_connexity = pd.read_sql_query('''SELECT * FROM connexity_spend;''', cnx)
    max_db_date_plus_one = str(pd.to_datetime(max(df_connexity['Date'])) + timedelta(1))[:10]
    print(max_db_date_plus_one)
    yesterday = str(datetime.today() - timedelta(1))[:10]
    
    if max_db_date_plus_one < yesterday:
        metrics= ['adCost','impressions','adClicks','transactions','transactionRevenue'] #'impressions','adClicks',
        dims = ['date','campaign','source']
        start_date = max_db_date_plus_one
        end_date = yesterday

        #  Make actual API call
        df_adwords = GA_api_call(start_date,end_date,metrics,dims)
        df_adwords['cost'] = df_adwords['ga:adCost'].astype(float)
        # df_adwords.groupby('ga:date',as_index=True)[['ga:adCost',
        #                                              'ga:impressions',
        #                                              'ga:adClicks']].apply(lambda x : x.astype(float).sum()).reset_index()
        new_cols = []
        for name in list(df_adwords.columns):
            new_cols.append(name.replace('ga:',''))
        df_adwords.columns = new_cols
        df_adwords['Date'] = df_adwords['date'].apply(convert_date)
        df_adwords = df_adwords.drop('date',1)
        df_adwords = df_adwords.drop('adCost',1)
        df_adwords = df_adwords.rename(columns={'campaign':'Campaign',
                                                  'adGroup':'Ad_Group',
                                                  'cost':'Spend',
                                                  'impressions':'Impressions',
                                                  'adClicks':'Clicks',
                                                  'transactions':'Conversions',
                                                  'transactionRevenue':'Revenue',
                                                  'source':'Channel'})
        df_adwords = df_adwords[df_adwords['Channel'].isin(['connexity', 'ebay_comm_net','google'])].copy()
        df_adwords['Channel'] = df_adwords['Channel'].apply(clean_channel)
        df_connexity_new = df_adwords[df_adwords['Channel']=='Connexity'].copy()
        df_connexity_new['Category'] = 'CSE'
        df_connexity_new['Tuesday_Week'] = df_connexity_new['Date'].apply(tuesday_week)


        # Append to existing table
        # sqlEngine = create_engine('mysql+pymysql://root:@127.0.0.1/test', pool_recycle=3600)
        dbConnection = cnx.connect()

        tableName = 'connexity_spend'

        try:
            frame = df_connexity_new.to_sql(tableName, dbConnection, if_exists='append');
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