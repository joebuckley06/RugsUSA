import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import sqlalchemy
import psycopg2
from sqlalchemy import create_engine
import json
from bingads.v13.reporting import *
from bingads import AuthorizationData
import authorization
from auth_helper import *
from bingads.v13.reporting import *

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

# Yesterday date
yesterday = str(datetime.today() - timedelta(1))[:10]
print(yesterday)
yesterday_year = int(yesterday[:4])
yesterday_month = int(yesterday[5:7])
yesterday_day = int(yesterday[8:10])

# You must provide credentials in auth_helper.py.

# The report file extension type.
REPORT_FILE_FORMAT='Csv'

# The directory for the report files.
FILE_DIRECTORY='/Users/joebuckley/Python/Rugs_USA/Database Updating'

# The name of the report download file.
RESULT_FILE_NAME='Bing_Ads_Data.' + REPORT_FILE_FORMAT.lower()

# The maximum amount of time (in milliseconds) that you want to wait for the report download.
TIMEOUT_IN_MILLISECONDS=3600000

def main(authorization_data):
    try:
        # You can submit one of the example reports, or build your own.

        report_request=get_report_request(authorization_data.account_id)
        
        reporting_download_parameters = ReportingDownloadParameters(
            report_request=report_request,
            result_file_directory = FILE_DIRECTORY, 
            result_file_name = RESULT_FILE_NAME, 
            overwrite_result_file = True, # Set this value true if you want to overwrite the same file.
            timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS # You may optionally cancel the download after a specified time interval.
        )

        #Option A - Background Completion with ReportingServiceManager
        #You can submit a download request and the ReportingServiceManager will automatically 
        #return results. The ReportingServiceManager abstracts the details of checking for result file 
        #completion, and you don't have to write any code for results polling.

        #output_status_message("-----\nAwaiting Background Completion...")
        #background_completion(reporting_download_parameters)

        #Option B - Submit and Download with ReportingServiceManager
        #Submit the download request and then use the ReportingDownloadOperation result to 
        #track status yourself using ReportingServiceManager.get_status().

        #output_status_message("-----\nAwaiting Submit and Download...")
        #submit_and_download(report_request)

        #Option C - Download Results with ReportingServiceManager
        #If for any reason you have to resume from a previous application state, 
        #you can use an existing download request identifier and use it 
        #to download the result file. 

        #For example you might have previously retrieved a request ID using submit_download.
        #reporting_operation=reporting_service_manager.submit_download(report_request)
        #request_id=reporting_operation.request_id

        #Given the request ID above, you can resume the workflow and download the report.
        #The report request identifier is valid for two days. 
        #If you do not download the report within two days, you must request the report again.
        #output_status_message("-----\nAwaiting Download Results...")
        #download_results(request_id, authorization_data)

        #Option D - Download the report in memory with ReportingServiceManager.download_report
        #The download_report helper function downloads the report and summarizes results.
        output_status_message("-----\nAwaiting download_report...")
        download_report(reporting_download_parameters)

    except WebFault as ex:
        output_webfault_errors(ex)
    except Exception as ex:
        output_status_message(ex)


def background_completion(reporting_download_parameters):
    """ You can submit a download request and the ReportingServiceManager will automatically 
    return results. The ReportingServiceManager abstracts the details of checking for result file 
    completion, and you don't have to write any code for results polling. """

    global reporting_service_manager
    result_file_path = reporting_service_manager.download_file(reporting_download_parameters)
    output_status_message("Download result file: {0}".format(result_file_path))

def submit_and_download(report_request):
    """ Submit the download request and then use the ReportingDownloadOperation result to 
    track status until the report is complete e.g. either using
    ReportingDownloadOperation.track() or ReportingDownloadOperation.get_status(). """

    global reporting_service_manager
    reporting_download_operation = reporting_service_manager.submit_download(report_request)

    # You may optionally cancel the track() operation after a specified time interval.
    reporting_operation_status = reporting_download_operation.track(timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS)

    # You can use ReportingDownloadOperation.track() to poll until complete as shown above, 
    # or use custom polling logic with get_status() as shown below.
    #for i in range(10):
    #    time.sleep(reporting_service_manager.poll_interval_in_milliseconds / 1000.0)

    #    download_status = reporting_download_operation.get_status()
        
    #    if download_status.status == 'Success':
    #        break
    
    result_file_path = reporting_download_operation.download_result_file(
        result_file_directory = FILE_DIRECTORY, 
        result_file_name = RESULT_FILE_NAME, 
        decompress = True, 
        overwrite = True,  # Set this value true if you want to overwrite the same file.
        timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS # You may optionally cancel the download after a specified time interval.
    )
    
    output_status_message("Download result file: {0}".format(result_file_path))

def download_results(request_id, authorization_data):
    """ If for any reason you have to resume from a previous application state, 
    you can use an existing download request identifier and use it 
    to download the result file. Use ReportingDownloadOperation.track() to indicate that the application 
    should wait to ensure that the download status is completed. """
    
    reporting_download_operation = ReportingDownloadOperation(
        request_id = request_id, 
        authorization_data=authorization_data, 
        poll_interval_in_milliseconds=1000, 
        environment=ENVIRONMENT,
    )

    # Use track() to indicate that the application should wait to ensure that 
    # the download status is completed.
    # You may optionally cancel the track() operation after a specified time interval.
    reporting_operation_status = reporting_download_operation.track(timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS)
    
    result_file_path = reporting_download_operation.download_result_file(
        result_file_directory = FILE_DIRECTORY, 
        result_file_name = RESULT_FILE_NAME, 
        decompress = True, 
        overwrite = True,  # Set this value true if you want to overwrite the same file.
        timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS # You may optionally cancel the download after a specified time interval.
    ) 

    output_status_message("Download result file: {0}".format(result_file_path))
    output_status_message("Status: {0}".format(reporting_operation_status.status))

def download_report(reporting_download_parameters):
    """ You can get a Report object by submitting a new download request via ReportingServiceManager. 
    Although in this case you will not work directly with the file, under the covers a request is 
    submitted to the Reporting service and the report file is downloaded to a local directory.  """
    
    global reporting_service_manager

    report_container = reporting_service_manager.download_report(reporting_download_parameters)

    #Otherwise if you already have a report file that was downloaded via the API, 
    #you can get a Report object via the ReportFileReader. 

    # report_file_reader = ReportFileReader(
    #     file_path = reporting_download_parameters.result_file_directory + reporting_download_parameters.result_file_name, 
    #     format = reporting_download_parameters.report_request.Format)
    # report_container = report_file_reader.get_report()

    if(report_container == None):
        output_status_message("There is no report data for the submitted report request parameters.")
        sys.exit(0)

    #Once you have a Report object via either workflow above, you can access the metadata and report records. 

    #Output the report metadata

    record_count = report_container.record_count
    output_status_message("ReportName: {0}".format(report_container.report_name))
    output_status_message("ReportTimeStart: {0}".format(report_container.report_time_start))
    output_status_message("ReportTimeEnd: {0}".format(report_container.report_time_end))
    output_status_message("LastCompletedAvailableDate: {0}".format(report_container.last_completed_available_date))
    output_status_message("ReportAggregation: {0}".format(report_container.report_aggregation))
    output_status_message("ReportColumns: {0}".format("; ".join(str(column) for column in report_container.report_columns)))
    output_status_message("ReportRecordCount: {0}".format(record_count))

    #Analyze and output performance statistics

    if "Impressions" in report_container.report_columns and \
        "Clicks" in report_container.report_columns and \
        "DeviceType" in report_container.report_columns and \
        "Network" in report_container.report_columns:

        report_record_iterable = report_container.report_records

        total_impressions = 0
        total_clicks = 0
        distinct_devices = set()
        distinct_networks = set()
        for record in report_record_iterable:
            total_impressions += record.int_value("Impressions")
            total_clicks += record.int_value("Clicks")
            distinct_devices.add(record.value("DeviceType"))
            distinct_networks.add(record.value("Network"))

        output_status_message("Total Impressions: {0}".format(total_impressions))
        output_status_message("Total Clicks: {0}".format(total_clicks))
        output_status_message("Average Impressions: {0}".format(total_impressions * 1.0 / record_count))
        output_status_message("Average Clicks: {0}".format(total_clicks * 1.0 / record_count))
        output_status_message("Distinct Devices: {0}".format("; ".join(str(device) for device in distinct_devices)))
        output_status_message("Distinct Networks: {0}".format("; ".join(str(network) for network in distinct_networks)))

    #Be sure to close the report.

    report_container.close()

def get_report_request(account_id):
    """ 
    Use a sample report request or build your own. 
    """

    aggregation = 'Daily'
    exclude_column_headers=False
    exclude_report_footer=False
    exclude_report_header=False
    time=reporting_service.factory.create('ReportTime')
    # You can either use a custom date range or predefined time.
#     time.PredefinedTime='Yesterday'
    custom_date_range_start=reporting_service.factory.create('Date')
    custom_date_range_start.Day=1
    custom_date_range_start.Month=1
    custom_date_range_start.Year=2019
    time.CustomDateRangeStart=custom_date_range_start
    
    custom_date_range_end=reporting_service.factory.create('Date')
    custom_date_range_end.Day=yesterday_day
    custom_date_range_end.Month=yesterday_month
    custom_date_range_end.Year=yesterday_year
    time.CustomDateRangeEnd=custom_date_range_end
    
    time.ReportTimeZone='EasternTimeUSCanada'
    return_only_complete_data=True

    #BudgetSummaryReportRequest does not contain a definition for Aggregation.
    budget_summary_report_request=get_budget_summary_report_request(
        account_id=account_id,
        exclude_column_headers=exclude_column_headers,
        exclude_report_footer=exclude_report_footer,
        exclude_report_header=exclude_report_header,
        report_file_format=REPORT_FILE_FORMAT,
        return_only_complete_data=return_only_complete_data,
        time=time)

    campaign_performance_report_request=get_campaign_performance_report_request(
        account_id=account_id,
        aggregation=aggregation,
        exclude_column_headers=exclude_column_headers,
        exclude_report_footer=exclude_report_footer,
        exclude_report_header=exclude_report_header,
        report_file_format=REPORT_FILE_FORMAT,
        return_only_complete_data=return_only_complete_data,
        time=time)

    keyword_performance_report_request=get_keyword_performance_report_request(
        account_id=account_id,
        aggregation=aggregation,
        exclude_column_headers=exclude_column_headers,
        exclude_report_footer=exclude_report_footer,
        exclude_report_header=exclude_report_header,
        report_file_format=REPORT_FILE_FORMAT,
        return_only_complete_data=return_only_complete_data,
        time=time)

    user_location_performance_report_request=get_user_location_performance_report_request(
        account_id=account_id,
        aggregation=aggregation,
        exclude_column_headers=exclude_column_headers,
        exclude_report_footer=exclude_report_footer,
        exclude_report_header=exclude_report_header,
        report_file_format=REPORT_FILE_FORMAT,
        return_only_complete_data=return_only_complete_data,
        time=time)

    return campaign_performance_report_request

def get_budget_summary_report_request(
        account_id,
        exclude_column_headers,
        exclude_report_footer,
        exclude_report_header,
        report_file_format,
        return_only_complete_data,
        time):

    report_request=reporting_service.factory.create('BudgetSummaryReportRequest')
    report_request.ExcludeColumnHeaders=exclude_column_headers
    report_request.ExcludeReportFooter=exclude_report_footer
    report_request.ExcludeReportHeader=exclude_report_header
    report_request.Format=report_file_format
    report_request.ReturnOnlyCompleteData=return_only_complete_data
    report_request.Time=time    
    report_request.ReportName="My Budget Summary Report"
    scope=reporting_service.factory.create('AccountThroughCampaignReportScope')
    scope.AccountIds={'long': [account_id] }
    scope.Campaigns=None
    report_request.Scope=scope     

    report_columns=reporting_service.factory.create('ArrayOfBudgetSummaryReportColumn')
    report_columns.BudgetSummaryReportColumn.append([
        'AccountName',
        'AccountNumber',
        'AccountId',
        'CampaignName',
        'CampaignId',
        'Date',
        'CurrencyCode',
        'MonthlyBudget',
        'DailySpend',
        'MonthToDateSpend'
    ])
    report_request.Columns=report_columns

    return report_request

def get_campaign_performance_report_request(
        account_id,
        aggregation,
        exclude_column_headers,
        exclude_report_footer,
        exclude_report_header,
        report_file_format,
        return_only_complete_data,
        time):

    report_request=reporting_service.factory.create('CampaignPerformanceReportRequest')
    report_request.Aggregation=aggregation
    report_request.ExcludeColumnHeaders=exclude_column_headers
    report_request.ExcludeReportFooter=exclude_report_footer
    report_request.ExcludeReportHeader=exclude_report_header
    report_request.Format=report_file_format
    report_request.ReturnOnlyCompleteData=return_only_complete_data
    report_request.Time=time    
    report_request.ReportName="My Campaign Performance Report"
    scope=reporting_service.factory.create('AccountThroughCampaignReportScope')
    scope.AccountIds={'long': [account_id] }
    scope.Campaigns=None
    report_request.Scope=scope     

    report_columns=reporting_service.factory.create('ArrayOfCampaignPerformanceReportColumn')
    report_columns.CampaignPerformanceReportColumn.append([
        'TimePeriod',
        'CampaignId',
        'CampaignName',
#         'DeviceType',
        'AdDistribution',
        'Network',
        'Spend',
        'Impressions',
        'Clicks',  
        'Conversions',
        'Revenue',
        'AllConversions',
        'AllRevenue'
    ])
    report_request.Columns=report_columns
    
    return report_request

def get_keyword_performance_report_request(
        account_id,
        aggregation,
        exclude_column_headers,
        exclude_report_footer,
        exclude_report_header,
        report_file_format,
        return_only_complete_data,
        time):

    report_request=reporting_service.factory.create('KeywordPerformanceReportRequest')
    report_request.Aggregation=aggregation
    report_request.ExcludeColumnHeaders=exclude_column_headers
    report_request.ExcludeReportFooter=exclude_report_footer
    report_request.ExcludeReportHeader=exclude_report_header
    report_request.Format=report_file_format
    report_request.ReturnOnlyCompleteData=return_only_complete_data
    report_request.Time=time    
    report_request.ReportName="My Keyword Performance Report"
    scope=reporting_service.factory.create('AccountThroughAdGroupReportScope')
    scope.AccountIds={'long': [account_id] }
    scope.Campaigns=None
    scope.AdGroups=None
    report_request.Scope=scope     

    report_columns=reporting_service.factory.create('ArrayOfKeywordPerformanceReportColumn')
    report_columns.KeywordPerformanceReportColumn.append([
        'TimePeriod',
        'AccountId',
        'CampaignId',
        'Keyword',
        'KeywordId',
        'DeviceType',
        'Network',
        'Impressions',
        'Clicks',  
        'Spend',
        'BidMatchType',              
        'Ctr',
        'AverageCpc',        
        'QualityScore'
    ])
    report_request.Columns=report_columns

    return report_request

def get_user_location_performance_report_request(
        account_id,
        aggregation,
        exclude_column_headers,
        exclude_report_footer,
        exclude_report_header,
        report_file_format,
        return_only_complete_data,
        time):
    
    report_request=reporting_service.factory.create('UserLocationPerformanceReportRequest')
    report_request.Aggregation=aggregation
    report_request.ExcludeColumnHeaders=exclude_column_headers
    report_request.ExcludeReportFooter=exclude_report_footer
    report_request.ExcludeReportHeader=exclude_report_header
    report_request.Format=report_file_format
    report_request.ReturnOnlyCompleteData=return_only_complete_data
    report_request.Time=time    
    report_request.ReportName="My User Location Performance Report"
    scope=reporting_service.factory.create('AccountThroughAdGroupReportScope')
    scope.AccountIds={'long': [account_id] }
    scope.Campaigns=None
    scope.AdGroups=None
    report_request.Scope=scope 

    report_columns=reporting_service.factory.create('ArrayOfUserLocationPerformanceReportColumn')
    report_columns.UserLocationPerformanceReportColumn.append([
        'TimePeriod',
        'AccountId',
        'AccountName',
        'CampaignId',
        'AdGroupId',
        'LocationId',
        'Country',
        'Clicks',
        'Impressions',
        'DeviceType',
        'Network',
        'Ctr',
        'AverageCpc',
        'Spend',
    ])
    report_request.Columns=report_columns

    return report_request


### JB functions ###

# Function to remove commas
def remove_comma(num_string):
    try:
        if ',' in num_string:
            return(num_string.replace(',',''))
        else:
            return(num_string)
    except:
        return(num_string)
    
import datetime
from datetime import timedelta
def standard_weekstart(string_date):
    return(pd.to_datetime(string_date).weekday())

def tuesday_weekstart(string_date):
    return((pd.to_datetime(string_date).weekday() - 1) % 7)

def tuesday_week(string_date):
    date_diff = (pd.to_datetime(string_date).weekday() - 1) % 7
    return(pd.to_datetime(string_date)-timedelta(date_diff))

# Credentials
# df_bing = pd.read_sql_query('''SELECT * FROM microsoft_spend;''', cnx)
# print(df_bing.tail())

# max_db_date_plus_one = str(pd.to_datetime(max(df_bing['Date'])) + timedelta(1))[:10]
# yesterday = str(datetime.datetime.today() - timedelta(1))[:10]
# yesterday_year = int(yesterday[:4])
# yesterday_month = int(yesterday[5:7])
# yesterday_day = int(yesterday[8:10])

campaign_map = pd.read_csv('RugsUSA_Campaign_Map.csv')
campaign_map = campaign_map[campaign_map['Channel']=='Bing'].copy()


#### UPDATE AND ADD TO AWS database
def update_AWS():
    # Download latest Database data
    df_bing = pd.read_sql_query('''SELECT * FROM microsoft_spend;''', cnx)
    
    # Get dates
    max_db_date_plus_one = str(pd.to_datetime(max(df_bing['Date'])) + timedelta(1))[:10]
    yesterday = str(datetime.datetime.today() - timedelta(1))[:10]
    yesterday_year = int(yesterday[:4])
    yesterday_month = int(yesterday[5:7])
    yesterday_day = int(yesterday[8:10])
    
    # Update if necessary
    if max_db_date_plus_one < yesterday:
        print("Date last updated: "+ max_db_date_plus_one)

        # Get new data
        print("Loading the web service client proxies...")
        authorization_data=AuthorizationData(
            account_id=None,
            customer_id=None,
            developer_token=DEVELOPER_TOKEN,
            authentication=None,
        )
        reporting_service_manager=ReportingServiceManager(
            authorization_data=authorization_data, 
            poll_interval_in_milliseconds=5000, 
            environment=ENVIRONMENT,
        )
        # In addition to ReportingServiceManager, you will need a reporting ServiceClient 
        # to build the ReportRequest.
        reporting_service=ServiceClient(
            service='ReportingService', 
            version=13,
            authorization_data=authorization_data, 
            environment=ENVIRONMENT,
        )
        authenticate(authorization_data)   
        main(authorization_data)

        # Import new data csv, clean, and add columns
        # Import current csv with microsoft API data
        df_bing = pd.read_csv('Bing_Ads_Data.csv',header=9)
        df_bing['AllRevenue'] = df_bing['AllRevenue'].fillna(0).apply(remove_comma).astype(float)
        df_bing['Revenue'] = df_bing['Revenue'].fillna(0).apply(remove_comma).astype(float)
        df_bing['AllConversions'] = df_bing['AllConversions'].astype(float)
        df_bing['Conversions'] = df_bing['Conversions'].astype(float)
        # remove Microsoft copyright at bottom of csv
        extra_text = '©2020 Microsoft Corporation. All rights reserved. '
        df_bing = df_bing[df_bing['TimePeriod']!= extra_text].copy()
        print(df_bing.tail())
        df_grouped = df_bing.groupby(['TimePeriod','CampaignName'],as_index=True)[['Spend','Impressions','Clicks',
                                                     'Revenue',
                                                     'Conversions']].sum().reset_index()
        df_grouped['Channel'] = 'Bing'
        df_grouped = df_grouped.rename(columns={'CampaignName':'Campaign'})
        df_grouped = df_grouped.merge(campaign_map,how='left',on=['Channel','Campaign'])
        df_grouped['Category'] = df_grouped['Category'].fillna('SEM - Text - Product')
        df_grouped = df_grouped[['Channel','TimePeriod', 'Campaign',  'Category','Spend', 'Impressions', 'Clicks',
               'Conversions', 'Revenue']].copy()
        df_grouped['Tuesday_Week'] = df_grouped['TimePeriod'].apply(tuesday_week)
        df_grouped = df_grouped.rename(columns={'TimePeriod':'Date'})

        # Append to existing table
        # sqlEngine = create_engine('mysql+pymysql://root:@127.0.0.1/test', pool_recycle=3600)
        dbConnection = cnx.connect()

        tableName = 'microsoft_spend'

        try:
            frame = df_grouped.to_sql(tableName, dbConnection, if_exists='replace');
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