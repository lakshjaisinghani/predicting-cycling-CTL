import glob
import gzip
import csv
import fitparse
import pandas as pd
import os

# Global file path
GLOBAL_FILEPATH = os.path.dirname(os.path.dirname(__file__))
CSV_DIR = os.path.join(GLOBAL_FILEPATH, "csv_data")
FILENAME = ""

activity = pd.DataFrame()
is_garmin = False
is_cycling = True
sport = ""
data = {}

def parse_record(values):
    data = {}
    req = ['cadence', 'power', 'speed', 'position_lat', 'position_long', 'altitude', 'timestamp']

    for key in values:
        if key in req:
            try:
                if key == 'position_lat' or key == "position_long":
                    # lat and long are polled by 31 bit adc
                    data[key] = int(values[key])/(2**31 / 180.00)
                elif key == "speed":
                    data[key] = (int(values[key])/ 1609344.00) * 3600
                elif key == "altitude":
                    data[key] = ((int(values[key])/ 5.00) - 500) * 3.2808399
                else:
                    data[key] = values[key]

            except:
                data[key] = float("NaN")
    return data


data_files = glob.glob("./data/[0-9]/*.fit.gz")
# data_files = data_files[258:]

print("Cleaning Data has begun !!")

for file in data_files:

    print("Processing : " + file)

    with gzip.open(file, 'rb') as f:
        file_content = f.read()

    fitfile = fitparse.FitFile(file_content)

    for message in fitfile.get_messages():

        if message.name == "file_id":
            manufacturer = message.get_values()['manufacturer']
            
            # convert from datetime object to str and then only keep yyyy-mm-dd
            FILENAME = str(message.get_values()['time_created'])[:10]
            
            if manufacturer == "garmin":
                is_garmin = True
            else:
                is_garmin = False
        
        if is_garmin and message.name == "sport":
            sport = message.get_values()['sport']

            if sport != 'cycling':
                is_cycling = False
            else:
                is_cycling = True
        
        if message.name == "record" and is_cycling:
            data = parse_record(message.get_values())
        
            # appending value to activity dataframe
            activity = activity.append(data, ignore_index=True)  

    # manipulate data frame
    activity = activity[["timestamp", "position_lat", "position_long", "altitude", "speed", "power", "cadence"]]

    # create csv
    csv_filename = f"data_{FILENAME}.csv"
    csv_filepath = os.path.join(CSV_DIR, csv_filename)

    with open(csv_filepath, mode='w') as csv_file:
        filewriter = csv.writer(csv_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)

    activity.to_csv(csv_filepath, index = False, header=True)
 