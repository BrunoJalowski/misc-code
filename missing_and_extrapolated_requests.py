#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  2 10:06:33 2025

@author: brunojalowski
"""
# Importing modules
import requests
import regex as re
import pandas as pd
from decouple import config

#%% Downloading filenames from json file

# Url for accessing server file names
API_REST_ADRESS = config('API_REST_ADRESS')

# Saving json file to python dict list 
json_data = requests.get(API_REST_ADRESS).json()

# Filtering 'controller.log*' files 
log_files = [data['name'] 
             for data in json_data
             if 'controller.log' in data['name']] 

#%% Downloading data 
FILE_SERVER = config('FILE_SERVER')

url_list = [FILE_SERVER + str(name) 
            for name in log_files]

# Listing all lines from all files
data = []
for url in url_list:
    # Getting data from url
    response = requests.get(url)
    data.append(response.text.split('\n'))

# Converting list of lists into flat list
txt_list = [x
            for xs in data 
            for x in xs]

# Iterating over lines and filtering target lines
lines = [txt_list[line]
         for line in range(len(txt_list))
         if '[INFO] [root] Process' in txt_list[line]]

# Selecting zoom and datetime (YYYY-MM-DD HH) from filtered lines 
valid_requests = [re.findall(r'(\s[\d]{2}\s)', line) +
                 re.findall(r'([\d]{4}\-[\d]{2}\-[\d]{2} [\d]{2})', line)
                 for line in lines]

valid_requests = sorted(valid_requests)


#%% REQUESTS THAT EXTRAPOLATED THEIR REPRESENTATIVE HOUR

# Filtering zoom and datetime that extrapolated their representative hour
extrapolated_requests = ["zoom" + req[0] + req[1] + ':00:00' 
                         for req in valid_requests
                         if req[1][-2:] != req[-1][-2:]]

# Listing zooms
extrapolated_zooms = [req.split(' ')[1]
                      for req in extrapolated_requests]

# Listing datetime
extrapolated_datetime = [pd.to_datetime(req.split(' ')[2] +
                                        ' ' +
                                        req.split(' ')[-1])
                         for req in extrapolated_requests]

#%% MISSING REQUESTS
valid_requests = ["zoom" + req[0] + req[1] + ':00:00'
                 for req in valid_requests]

# Defining temporal boundaries
unique_valid_requests = sorted(list(set(pd.to_datetime(data[7:])
                                       .timestamp()
                                       for data in valid_requests)))

# Creating reference list of hourly datetime values  
datetime_ref = pd.date_range(
    start = pd.to_datetime(min(unique_valid_requests),
                           unit = 's'),
    end = pd.to_datetime(max(unique_valid_requests),
                         unit = 's'),
    freq = 'h')

# Converting from timestamp to datetime
unique_valid_requests = pd.to_datetime(unique_valid_requests, unit='s') 


missing_requests = [req
                    for req in datetime_ref
                    if req not in unique_valid_requests]









#%% Defining function for verifying missing and extrapoled requests

# Requirements
import requests
import regex as re
import pandas as pd
from decouple import config

# Url for accessing log names
API_REST_ADRESS = config('API_REST_ADRESS')
    
# Url for accessing each log file's data
FILE_SERVER = config('FILE_SERVER')


def missing_and_extrapolated_requests(API_REST_ADRESS, FILE_SERVER): 
    """
    Identifies and returns lists with zoom and datetime values of requests
    that extrapolated the hour they should represent, and datetime values of
    missing requests.
    
    Parameters
    ----------
    API_REST_ADRESS: str
        Url for accessing log names
    
    FILE_SERVER: str
        Url for accessing each log file's data
    
    Returns
    -------
    extrapolated_datetime : list of datetime
        List os datetime values of extrapolated requests, in chronological 
        order.
            
    extrapolated_zooms : list of strings
        List of zoom values of extrapolated requests, with same index as
        the datetime list.
    missing_requests : list of datetime
        List of datetime values of the missing requests.
        
    """    
    # Saving json file to python dict list 
    json_data = requests.get(API_REST_ADRESS).json()

    # Filtering 'controller.log*' files 
    log_files = [data['name'] 
                 for data in json_data
                 if 'controller.log' in data['name']] 
    
    
    url_list = [FILE_SERVER + str(name) 
                for name in log_files]

    
    # Listing all lines from all files
    data = []
    for url in url_list:
        # Getting data from url
        response = requests.get(url)
        data.append(response.text.split('\n'))

    # Converting list of lists into flat list
    txt_list = [x
                for xs in data 
                for x in xs]

    # Iterating over lines and filtering target lines
    lines = [txt_list[line]
             for line in range(len(txt_list))
             if '[INFO] [root] Process' in txt_list[line]]

    # Selecting zoom and datetime (YYYY-MM-DD HH) from filtered lines 
    valid_requests = [re.findall(r'(\s[\d]{2}\s)', line) +
                     re.findall(r'([\d]{4}\-[\d]{2}\-[\d]{2} [\d]{2})', line)
                     for line in lines]

    valid_requests = sorted(valid_requests)
    
    # ----------------- EXTRAPOLATED REQUESTS ----------------------------
    
    # Filtering zoom and datetime that extrapolated their representative hour
    extrapolated_requests = ["zoom" + req[0] + req[1] + ':00:00' 
                             for req in valid_requests
                             if req[1][-2:] != req[-1][-2:]]

    # Listing zooms
    extrapolated_zooms = [req.split(' ')[1]
                          for req in extrapolated_requests]

    # Listing datetime
    extrapolated_datetime = [pd.to_datetime(req.split(' ')[2] +
                                            ' ' +
                                            req.split(' ')[-1])
                             for req in extrapolated_requests]
    
    # ----------------- MISSING REQUESTS ---------------------------------

    valid_requests = ["zoom" + req[0] + req[1] + ':00:00'
                     for req in valid_requests]

    # Defining temporal boundaries
    unique_valid_requests = sorted(list(set(pd.to_datetime(data[7:])
                                           .timestamp()
                                           for data in valid_requests)))

    # Creating reference list of hourly datetime values  
    datetime_ref = pd.date_range(
        start = pd.to_datetime(min(unique_valid_requests),
                               unit = 's'),
        end = pd.to_datetime(max(unique_valid_requests),
                             unit = 's'),
        freq = 'h')

    # Converting from timestamp to datetime
    unique_valid_requests = pd.to_datetime(unique_valid_requests, unit='s') 


    missing_requests = [req
                        for req in datetime_ref
                        if req not in unique_valid_requests]
    
    return (extrapolated_datetime,
            extrapolated_zooms,
            missing_requests) 
