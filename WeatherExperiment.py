#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 21 14:54:06 2018

@author: mead
"""

from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
import time
import requests
import numpy as np
from lxml import etree
import pandas as pd
from pandas import DataFrame, Series
import datetime
import sys
import json
import boto3
import io
import gzip

#########################
## WEATHER UNDERGROUND ##
#########################
#wundergroundDict = {'Charlottesville' : '22901'}

def funcScrapeTableWunderground(html_tree, forecast_date_str):
    """
    
    """
    # This will get you the Wunderground table headers for future hour conditions
    columns = html_tree.xpath("//table[@id='hourly-forecast-table']/thead//button[@class='tablesaw-sortable-btn']")
    rows = html_tree.xpath("//table[@id='hourly-forecast-table']/tbody/tr")
    fill_cols = np.asarray([])
    for column in columns:
    #    print etree.tostring(column)
        col = column.xpath("text()")[0]
        fill_cols = np.append(fill_cols, col)
    #    print(col)
    
    # Make a DataFrame to fill
    dayDf = DataFrame(columns = fill_cols)#.set_index(fill_cols[0])
    
    # This will go through the rows of the table and grab actual values
    for row in rows:
        values = row.xpath("td")
        for i, value  in enumerate(values):
            col = columns[i].xpath("text()")[0]
            val = value.xpath("ng-saw-cell-parser/div//span/text()")
#            print(val)
            if col == 'Time':
                timeVal = val
                # Initializing a single row. The goal is to make it look just like what dayDf looks like
                hourRow = pd.DataFrame([forecast_date_str + ' ' + (''.join(timeVal))], 
                                        columns = [col])#.set_index
            elif col == 'Conditions':
                hourRow[col] = val[1]
            else:
                if col == 'Pressure':
                    val = value.xpath("ng-saw-cell-parser//span/span/text()")
                    val = [val[0] + ' ' + val[2][0:2]]
                if col in ['Precip', 'Amount']: # These are hiding behind hyperlinks. Need to be smart
                    val = value.xpath("ng-saw-cell-parser/div//span/a/text()")
                try:
                    hourRow[col] = val[0]
                except:
                    hourRow[col] = np.nan
        dayDf = dayDf.append(hourRow)
        dayDf['Time'] = pd.to_datetime(dayDf['Time'])
    #        print(columns[i].xpath("text()")[0])
    #        print value.xpath("ng-saw-cell-parser/div//span/text()")
    return dayDf

def funcScrapeAllTablesWunderground(location, base_url = 'https://www.wunderground.com'):
    """
    
    """
    tenDayDf = DataFrame()
    
    # This portion is Selenium
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    browser = webdriver.Chrome(chrome_options=chrome_options)

    url = base_url + "/hourly/us/va/" + location + "?cm_ven=localwx_hour"
    browser.get(url) #navigate to the page
#    time.sleep(5)
    # The first time very explicitly grab today's forecast
    # This portion is lxml
    innerHTML = browser.execute_script("return document.body.innerHTML") #returns the inner HTML as a string
    # Make it into a parse-able tree
    html_tree = etree.HTML(innerHTML)
    # Getting what day it's forecasting for and the date we're in right now
    full_date = html_tree.xpath("//*[@id='forecast-title-long']/text()")[0]
    if "Today" in full_date:
        as_of_time = full_date[-5:]
    forecast_date = full_date[-5:] # The month-day
    retrieval_time = datetime.datetime.now()
    as_of_date = retrieval_time.replace(microsecond=0,second=0,minute=0) # What hour we're taking the forecasts from
    as_of_master = retrieval_time.replace(microsecond=0,second=0,minute=0)
    forecast_year = as_of_date.year # The year. Useful at end of December
    
    early_forecast_date_dtime = datetime.datetime.strptime(str(forecast_year) + '/' + forecast_date,
                                                     '%Y/%m/%d')
    
    # Scrape the single table
    dayDf = funcScrapeTableWunderground(html_tree, 
                                        early_forecast_date_dtime.strftime("%Y-%m-%d"))
    dayDf['As Of'] = as_of_date
    dayDf['Location'] = location
    dayDf['Service'] = 'Weather Underground'
    dayDf['Access Time'] = retrieval_time
    
    tenDayDf = tenDayDf.append(dayDf)
    

    # Go through the other days
    for i in range(10): # We can press the "Next Day Hourly Forecast" button up to 10       
        print(i + 1 )
        
        forecast_date_dtime = early_forecast_date_dtime + datetime.timedelta(days = i + 1)
        # Go to future days
        url = base_url + "/hourly/us/va/" + location + "/date/" +\
        forecast_date_dtime.strftime("%Y-%m-%d") + "?cm_ven=localwx_hour"
        
        browser.get(url) #navigate to the page
        time.sleep(5)

        # This portion is lxml
        innerHTML = browser.execute_script("return document.body.innerHTML") #returns the inner HTML as a string
        # Make it into a parse-able tree
        html_tree = etree.HTML(innerHTML)

        # Getting what day it's forecasting for and the date we're in right now
        full_date = html_tree.xpath("//*[@id='forecast-title-long']/text()")[0]
        if "Today" in full_date:
            as_of_time2 = full_date[-5:]
            assert as_of_time == as_of_time2
        forecast_date = full_date[-5:]
        retrieval_time = datetime.datetime.now()
        as_of_date = retrieval_time.replace(microsecond=0,second=0,minute=0) # What hour we're taking the forecasts from
        assert as_of_master == as_of_date
        forecast_year = as_of_date.year # The year
        
        # Now scrape
        dayDf = funcScrapeTableWunderground(html_tree, 
                                            forecast_date_dtime.strftime("%Y-%m-%d"))
    
        dayDf['As Of'] = as_of_date
        dayDf['Location'] = location
        dayDf['Service'] = 'Weather Underground'
        dayDf['Access Time'] = retrieval_time
        
        tenDayDf = tenDayDf.append(dayDf)
    
    tenDayDf = tenDayDf.set_index(['As Of', 'Time', 'Location', 'Service'])
    
    browser.quit()
    
    return tenDayDf

wundergroundDf = funcScrapeAllTablesWunderground('Charlottesville')

wundergroundSchema = {'index_names' : ['as_of_hr', 'fcast_hr', 'loc', 'serv'],
                      'column_names' : {'Conditions' : 'conds', 'Temp.' : 'temp', 
                                        'Feels Like' : 'feels_like', 'Precip' : 'precip_prob', 
                                        'Amount' : 'precip_amt', 'Cloud Cover' : 'cloud_cover_pct', 
                                        'Dew Point' : 'dew_pnt', 'Humidity' : 'humid_pct', 
                                        'Wind' : 'wind_spd', 'Pressure' : 'pr', 
                                        'Access Time' : 'access_time'},
                      'column_order' : ['conds', 'temp', 'temp_units', 'feels_like', 
                                        'precip_prob', 'precip_amt', 'precip_amt_units', 
                                        'cloud_cover_pct', 'dew_pnt', 'dew_pnt_units', 
                                        'humid_pct', 'wind_spd', 'wind_spd_units', 
                                        'wind_dir', 'pr', 'pr_units', 'access_time']}
def funcStandardizeWunderground(df, schema):
    """
    
    """
    df = df.copy()
    # Rename the index
    df.index.rename(schema['index_names'], inplace = True)
    # Rename the data columns
    df.rename(columns = schema['column_names'], inplace = True)
    # Now we need to derive a couple more columns
    # Also going to do a lot of error checking
    df['temp_units'] = df.apply(lambda x : x.loc['temp'][-1], axis = 1)
    assert len(df['temp_units'].unique()) == 1   # Only want temp reported as one value
    assert df['temp_units'].unique()[0] in ['F', 'C', 'K']  # Should be Fahrenheit, Celsius, or Kelvin
    feels_like_units = df.apply(lambda x : x.loc['feels_like'][-1], axis = 1)  # Check the uit in the feels_like col
    assert np.all(df['temp_units'].values == feels_like_units.values) # Should be the same unit as for temp
    df['temp'] = df.apply(lambda x : x.loc['temp'].split(' ')[0], axis = 1)   # Grab the number
    try: # And make sure it's actually an integer
        df['temp'] = df['temp'].astype('int')
    except ValueError:
        print("Uh oh! The temp column could not be made into an integer.")
    df['feels_like'] = df.apply(lambda x : x.loc['feels_like'].split(' ')[0], axis = 1) # Repeat for feels_like
    try:
        df['feels_like'] = df['feels_like'].astype('int')
    except ValueError:
        print("Uh oh! The feels_like column could not be made into an integer.")
    df['precip_prob'] = df.apply(lambda x : x.loc['precip_prob'].split('%')[0], axis = 1) # Get rid of the % sign
    try:
        df['precip_prob'] = df['precip_prob'].astype('int')
    except ValueError:
        print("Uh oh! The precip_prob column could not be made into an integer.")
    df['precip_amt_units'] = df.apply(lambda x : x.loc['precip_amt'].split(' ')[-1], axis = 1) # Pull out the units
    assert len(df['precip_amt_units'].unique()) == 1   # Only want temp reported as one value
    assert df['precip_amt_units'].unique()[0] in ['in', 'cm']  # Should be inches or centimeters
    df['precip_amt'] = df.apply(lambda x : x.loc['precip_amt'].split(' ')[0], axis = 1)
    try:
        df['precip_amt'] = df['precip_amt'].astype('float')
    except ValueError:
        print("Uh oh! The precip_amt column could not be made into a float.")
    df['cloud_cover_pct'] = df.apply(lambda x : x.loc['cloud_cover_pct'].split('%')[0], axis = 1) # Get rid of the % sign
    try:
        df['cloud_cover_pct'] = df['cloud_cover_pct'].astype('int')
    except ValueError:
        print("Uh oh! The cloud_cover_pct column could not be made into an integer.")
    df['dew_pnt_units'] = df.apply(lambda x : x.loc['dew_pnt'][-1], axis = 1)
    assert len(df['dew_pnt_units'].unique()) == 1   # Only want dew point reported as one value
    assert df['dew_pnt_units'].unique()[0] in ['F', 'C', 'K']  # Should be Fahrenheit, Celsius, or Kelvin
    df['dew_pnt'] = df.apply(lambda x : x.loc['dew_pnt'].split(' ')[0], axis = 1)   # Grab the number
    try: # And make sure it's actually an integer
        df['dew_pnt'] = df['dew_pnt'].astype('int')
    except ValueError:
        print("Uh oh! The dew_pnt column could not be made into an integer.")
    df['humid_pct'] = df.apply(lambda x : x.loc['humid_pct'].split('%')[0], axis = 1) # Get rid of the % sign
    try:
        df['humid_pct'] = df['humid_pct'].astype('int')
    except ValueError:
        print("Uh oh! The humid_pct column could not be made into an integer.")
    df['wind_spd_units'] = df.apply(lambda x : x.loc['wind_spd'].split(' ')[1], axis = 1) # Pull out the units
    assert len(df['wind_spd_units'].unique()) == 1   # Only want wind speed units reported as one value
    assert df['wind_spd_units'].unique()[0] in ['mph', 'cm']  # Should be miles-per-hour or kilometers-per-hour
    df['wind_dir'] = df.apply(lambda x : x.loc['wind_spd'].split(' ')[2], axis = 1) # Pull out the units
    assert len(df['wind_dir'].unique()) <= 16   # Want wind speeds in one of the 16 cardinal directions
    assert np.all(df['wind_dir'].apply(lambda x : x in ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 
                                                          'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 
                                                          'W', 'WNW', 'NW', 'NNW']))  # Should be a compass direction
    df['wind_spd'] = df.apply(lambda x : x.loc['wind_spd'].split(' ')[0], axis = 1) # Repeat for feels_like
    try:
        df['wind_spd'] = df['wind_spd'].astype('int')
    except ValueError:
        print("Uh oh! The wind_spd column could not be made into an integer.")
    df['pr_units'] = df.apply(lambda x : x.loc['pr'].split(' ')[-1], axis = 1) # Pull out the units
    assert len(df['pr_units'].unique()) == 1   # Only want pressure reported with one unit
    assert df['pr_units'].unique()[0] in ['in', 'cm']  # Should be inches or centimeters
    df['pr'] = df.apply(lambda x : x.loc['pr'].split(' ')[0], axis = 1)
    try:
        df['pr'] = df['pr'].astype('float')
    except ValueError:
        print("Uh oh! The pr column could not be made into a float.")
    
    df = df[schema['column_order']]
    
    return df
standWundergroundDf = funcStandardizeWunderground(wundergroundDf, wundergroundSchema)


def pandas_to_s3(df, client, bucket, key):
    """
    
    """
    # write DF to string stream
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # reset stream position
    csv_buffer.seek(0)
    # create binary stream
    gz_buffer = io.BytesIO()

    # compress string stream using gzip
    with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
        gz_file.write(bytes(csv_buffer.getvalue(), 'utf-8'))
        
    # write stream to S3
    obj = client.put_object(Bucket=bucket, Key=key, Body=gz_buffer.getvalue())

# This is what the file will be saved as
use_key = str(standWundergroundDf.iloc[0].name[0]) + ' -- ' +  \
              standWundergroundDf.iloc[0].name[2] + ' -- ' + \
              standWundergroundDf.iloc[0].name[3] + '.csv.gz'

# Open up a connection to s3
session = boto3.Session(profile_name='dev')
dev_s3_client = session.client('s3')

# Write the DF to s3
pandas_to_s3(standWundergroundDf, dev_s3_client, 'adrian-mead-weather-forecasts', use_key)

#s3 = session.resource('s3')
#for bucket in s3.buckets.all():
#    print(bucket.name)


##########
## NOAA ##
##########
noaa_dict = {'Charlottesville' : ['38.03', '-78.48']}



def funcNOAAapi(location, noaa_dict, base_url = 'https://api.weather.gov/', 
                user_agent = {'User-agent': 'Mozilla/5.0'}):
    """
    
    """
    apiCall = base_url + 'points/' + noaa_dict[location][0] + ',' + noaa_dict[location][1] + '/forecast/hourly'

    retrieval_time = datetime.datetime.now()
    as_of_date = retrieval_time.replace(microsecond=0,second=0,minute=0) # What hour we're taking the forecasts from
    
    response = requests.get(apiCall, headers=user_agent)
    json_data = json.loads(response.text)
    
    # All the data lives here: ['properties']['periods']
    hours_of_interest = json_data['properties']['periods']
    assert len(hours_of_interest) == 156
    # Make a DataFrame to be populated
    sevenDayDf = DataFrame()
    for hour in hours_of_interest[1:]: # We don't need the first hour
        newHour = DataFrame(hour, index = [0])
        sevenDayDf = sevenDayDf.append(newHour)
    assert sevenDayDf.shape == (155, 13) # Just making sure we expect the dimensions we're getting
    # Only keep a subset of columns
    sevenDayDf.drop(['detailedForecast', 'endTime', 'icon', 'isDaytime', 'number', 
                     'temperatureTrend', 'name'], axis = 1, inplace = True)
    # And make it into a datetime
    sevenDayDf['startTime'] = pd.to_datetime(sevenDayDf['startTime'].apply(lambda x: x[:-6]))
    # Other useful columns
    sevenDayDf['As Of'] = as_of_date
    assert as_of_date + datetime.timedelta(hours = 1) == sevenDayDf.iloc[0].loc['startTime'].to_pydatetime()
    sevenDayDf['Access Time'] = retrieval_time
    sevenDayDf['Location'] = location
    sevenDayDf['Service'] = 'NOAA'

    sevenDayDf = sevenDayDf.set_index(['As Of', 'startTime', 'Location', 'Service'])
    
    return sevenDayDf

noaaDf = funcNOAAapi('Charlottesville', noaa_dict)
"""
Here's the To-Do:
    1. Decide on the cities I want to do.
        Charlottesville, New York City, Los Angeles, Houston, Philadelphia,
        Phoenix, Jacksonville, Seattle, Milwaukee, Anchorage
    
    2. Decide on the services I want to do.
        Weather Underground, NOAA, WeatherBug, Intellicast, Yahoo!
        
    3. Standardize how the data are formatted.
    | as_of_hr | fcast_hr | loc | serv | conds | temp | feels_like | temp_units | 
    precip_prob | precip_amt | precip_amt_units | cloud_cover_pct | 
    dew_pnt | dew_pnt_units | humid_pct | wind_spd | wind_spd_units | wind_dir |
    pr | pr_units | access_time
    
    4. Setup something like SQLite databases. -- boto3 into s3
    
    
    5. Scrape historical weather data.
    
    
    6. Access the Archive to get some old examples (if possible)
"""