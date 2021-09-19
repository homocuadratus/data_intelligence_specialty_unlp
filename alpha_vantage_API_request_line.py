############################### MODULES ###############################

import numpy as np

import datetime
from influxdb_client import InfluxDBClient, WritePrecision, WriteOptions
from influxdb_client.client.write_api import ASYNCHRONOUS, SYNCHRONOUS, PointSettings
from pytz import UTC
from influxdb_client.client.write.point import EPOCH
import ciso8601

from alpha_vantage.timeseries import TimeSeries

############################## PARAMETERS ##############################

apikeys = {"alejo": "QQWAHPE5V4QGJ5V7"}

parameters = {
    "alpha_vantage": {
        "key": apikeys["alejo"], # Claim your free API key here: https://www.alphavantage.co/support/#api-key
        "symbol": "IBM",
        "outputsize": "full",
        "key_adjusted_close": "5. adjusted close",
    },
    "influx_client":{
        "url": "http://localhost:8086",
        "token": "lfxC2lYwGfnLPGt7viM80qZASpVFwu7z0ELorZEdAqyw5AH94V7TfgrpM8ga-vhtLd0LkxN2i5uKQ82_p2V7lg==",
        "org": "influxProject",
        "timeout" : 300000
    },
    "influx_write_api":{
        "bucket": "alphaVantageTest",
        "batch_size": 800,
        "flush_interval": 10000
    }
}

############################## FUNCTIONS ##############################

def retrieve_daily_adjusted_data(parameters):
    
    '''
    parameters: dictionary containing the parameters to connect and retrieve information from Alpha Vantage API
    
    returns: data and meta_data dictionaries
    '''
    
    ts = TimeSeries(key=parameters["alpha_vantage"]["key"])
    data, meta_data = ts.get_daily_adjusted(parameters["alpha_vantage"]["symbol"], outputsize=parameters["alpha_vantage"]["outputsize"])
    
    return data, meta_data

def daily_adjusted_data_to_line(data, meta_data):

    '''

    data: dictionary containing the Alpha Vantage API request's response

    meta_data: dictionary containing the Alpha Vantage API request's metadata

    returns: list with measurements in line protocol 

    '''

    observation = [value for key, value in data.items()]
    observation.reverse()
    observation_number = len(observation)

    tag_key = [key[3:] for key, value in meta_data.items()]
    tag_value = [value for key, value in meta_data.items()]

    field_name = [key for key, value in observation[0].items()]
    field_number = len(field_name)

    unix_ns_timestamp = [str(int((UTC.localize(ciso8601.parse_datetime(date)) - EPOCH).total_seconds() * 1e9)) for date in data.keys()]
    unix_ns_timestamp.reverse()

    measurement_name = str(f"{tag_value[0]},".replace(" ", "_"))

    tags = ''

    lines = []

    for m in [1,4]:                                # THIS INDEXES HAVE TO BE ADAPTED BY HAND HAND FOR EACH METRIC OF INTEREST
        tags += f"{tag_key[m]}={tag_value[m]}," 

    tags = tags[:-1].replace(" ", "_")

    for k in range(0, observation_number):
        fields = ''
        
        for l in range(0, field_number):
            fields += f'{field_name[l][3:]}='.replace(" ", "_") + observation[k][f'{field_name[l]}']+ ","

        fields = fields[:-1]

        line = str(measurement_name
             + tags
             + " "
             + fields
             + " "
             + unix_ns_timestamp[k])

        lines.append(line)
    return lines

############################## ALPHA VANTAGE API REQUEST ##############################

data, meta_data = retrieve_daily_adjusted_data(parameters)

############################## DATA TRANSFORMATION TO LINE PROTOCOL ###################

lines = daily_adjusted_data_to_line(data, meta_data)

############################## DATA WRITING TO INFLUX DB ##############################

with InfluxDBClient(url=parameters["influx_client"]["url"], token=parameters["influx_client"]["token"], org=parameters["influx_client"]["org"], timeout=parameters["influx_client"]["timeout"], enable_gzip=False, debug=True) as client:
    
    with client.write_api(write_options=WriteOptions(batch_size=parameters["influx_write_api"]["batch_size"])) as write_api:
        
        write_api.write(bucket=parameters["influx_write_api"]["bucket"], record=lines)

