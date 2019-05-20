"""
Crawl travel time from Google API
Request travel times for OD pairs with departure time as 'now'.

Inputs:
RunTimeRange_Start: = The start of time range for travel time requesting, as UTC Time!!!
RunTimeRange_End: The end of time range for travel time requesting, as UTC Time!!!
input_file_now:  Address of the file which has a list of coordinates for OD pairs (origins|destinations)
RouteName: Name of the OD pair list for requesting, to be used in the name of final output file
PointMax: Max number limit of requesting
api_key: Google API key.

Output file columns:
Req_Date: date of the request (also the date of departure)
Req_Time: time of the request (also the time of departure)
Distance: distance of the route chosen for the OD
TT_Pessimistic: pessimistic travel time
TT_Best_Guess: best guess of travel time
TT_Optimistic: optimistic travel time

@By Danny Wang, 2018/06/07, Northeastern University

v1.1: Removed requests for Pessimistic and Optimistic travel time.
"""

import urllib.request
import json
import pandas as pd
import os
import csv
import simplejson
import urllib
from datetime import datetime, timedelta
import time
import threading
import multiprocessing
import pdb
%run OftenUsedPackages.ipynb

def crawl_data_google(request_date, input_data, timenow_obj, api_key):
    """
    :param input_data: pandas dataframe with origins|destinations|{dep_date|dep_time|dep_epoch}
    :param output_file: output csv file name to store the extracted travel times
    :return: DataFrame
    """
    # parameter settings
    url_prex_distance_matrix = 'https://maps.googleapis.com/maps/api/distancematrix/json?'
    travel_mode = 'driving'
    list_traffic_model = ['best_guess']

    # Read and write files
    df_request = pd.DataFrame(columns=['ID', 'Origins', 'Destinations', 'Req_Date', 'Req_Time', 'Distance', 'Duration',
                                'TT_Best_Guess'])
    
    # crawl travel time from APIs
    for index, info in input_data.iterrows():
        origins = info.origins
        destinations = info.destinations
        request_time = timenow_obj
        
        try:
            departure_date = info.dep_date
            departure_time = info.dep_time
        except:
            departure_date = request_date
            departure_time = request_time

        travel_time = pd.DataFrame(index=[0], columns=list_traffic_model)
        distance = 0
        duration = 0
        for traffic_model in list_traffic_model:
           
            departure_epoch = 'now'           
            nav_request = 'origins={}&destinations={}&key={}&departure_time={}&mode={}&traffic_model={}'.format(
                    origins, destinations, api_key, departure_epoch, travel_mode, traffic_model)
            request = url_prex_distance_matrix + nav_request
            print(request)
            status = 'INVALID URL REQUEST:' + request
            try:
                result_request = simplejson.load(urllib.request.urlopen(request))
                
                status = result_request['status']
                # Get the driving time in seconds
                duration = result_request['rows'][0]['elements'][0]['duration']['value']
                duration_in_traffic = result_request['rows'][0]['elements'][0]['duration_in_traffic']['value']
                distance = result_request['rows'][0]['elements'][0]['distance']['value']

                column_name = traffic_model
                travel_time.loc[0, column_name] = duration_in_traffic
            except:
                # print error information, either invalid url request or error message returned by API
                print(status)
                continue

        # write the results to csv file
        data_output = [info.ID, origins, destinations, request_date, request_time.strftime('%H:%M:%S'), distance, duration] + \
                      list(travel_time.values[0])
        df_request_tmp = pd.DataFrame([data_output], columns=df_request.columns)        
        df_request = df_request.append(df_request_tmp)
        #pdb.set_trace()
        
    return df_request

    
# Travel times for OD pairs with departure time now
def run_now(input_file_now, timenow_obj, api_key):

    request_date = timenow_obj.strftime('%Y-%m-%d')
    request_time = timenow_obj.strftime('%H:%M:%S')
    print('run_now is running for UTC time:' + request_date + ' ' + request_time)
    
    input_file_name = input_file_now.split('/')[-1].split('.')[0]
    
    input_data_now = pd.read_csv(input_file_now).dropna()
    df_request = crawl_data_google(request_date, input_data_now, timenow_obj, api_key)

    return df_request
	
	
# Main Function

# ---------------------------------------Inputs----------------------------------------
# UTC Time range for travel time requesting
RunTimeRange_Start = '2019-05-03 06-00-00'  # As UTC Time!!!
RunTimeRange_End = '2019-05-03 22-00-00'  # As UTC Time!!!
# Coordinates file of OD pairs
input_file_now = 'InputCoord.csv'
# input_file_now = 'OD Pairs/InputCoord_test.csv'
RouteName = 'LCAP2445'  # The beginning of the name for final output file
# Max number of requests for the run and API key
PointMax = 9999999999  # Set this as a big number if terminating by time range
# api_key = '???????????????????????????????????'  # Your API key here

# Requesting frequency:
ReqFreq = 100  # in seconds (Request data every 100s, e.g.)

# ------------------------------------End of Inputs------------------------------------

# Initialize final output tables
Output_ColumnNames = ['ID', 'Origins', 'Destinations', 'Req_Date', 'Req_Time', 'Distance', 'Duration',
                                'TT_Best_Guess']
GoogleTimeNow_Table = pd.DataFrame(columns=Output_ColumnNames)

# Pre-calculation for time objects
RunTimeRange_Start_obj = datetime.strptime(RunTimeRange_Start, "%Y-%m-%d %H-%M-%S")
RunTimeRange_End_obj = datetime.strptime(RunTimeRange_End, "%Y-%m-%d %H-%M-%S")

# Initial values for iterations
Flag_Terminate = False
PointCnt = 1

# Main loop
while Flag_Terminate == False:
    timenow_obj = datetime.utcnow()  # Generate time object of time now
    Flag_Start = (timenow_obj >= RunTimeRange_Start_obj)  # Check if the travel time requesting should start
    
    if Flag_Start == True:
        
        TimeNow_str = timenow_obj.strftime("%H-%M-%S")
        timedelta = timenow_obj - RunTimeRange_Start_obj
        if timedelta.seconds % ReqFreq == 0:
            
            # Get the crawled data
            df_request = run_now(input_file_now, timenow_obj, api_key)
            
            # Append new results
            GoogleTimeNow_Table = GoogleTimeNow_Table.append(df_request)
            PointCnt += len(df_request)
            time.sleep(1)

        # Check terminating conditions
        if PointCnt >= PointMax or timenow_obj >= RunTimeRange_End_obj:
            Flag_Terminate = True  # Travel time requesting will terminate after this iteration

# Save results into csv file
output_file = 'Google_TT_' + RouteName + ' ' + RunTimeRange_Start + ' to ' + RunTimeRange_End + '.csv'
GoogleTimeNow_Table.to_csv(output_file, index=False)
