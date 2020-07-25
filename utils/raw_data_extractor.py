import glob
import gzip
import csv
import fitparse
import pandas as pd
import os

# Global file path
GLOBAL_FILEPATH = os.path.dirname(os.path.dirname(__file__))
CSV_DIR = os.path.join(GLOBAL_FILEPATH, "csv_data")

activity = pd.DataFrame()
is_garmin = False
is_cycling = True
sport = ""
data = {}

def parse_record(values):
    data = {}
    req = ['cadence', 'power', 'speed', 'position_lat', 'position_long', 'timestamp']

    for key in values:
        if key in req:
            try:
                data[key] = values[key]
            except:
                data[key] = float("NaN")
    return data

# work with one file
file = "data/2/laksh-jaisinghani.2020-07-19-00-35-12-184Z.GarminPush.61641281782.fit.gz"
with gzip.open(file, 'rb') as f:
    file_content = f.read()

fitfile = fitparse.FitFile(file_content)


for message in fitfile.get_messages():

    if message.name == "file_id":
        manufacturer = message.get_values()['manufacturer']
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
print(activity.head(10))

# create csv
num = "1" # find a better name

# Ensures that the temp file is in the same folder as the script
csv_filename = f"data_{num}.csv"
csv_filepath = os.path.join(CSV_DIR, csv_filename)

with open(csv_filepath, mode='w') as csv_file:
    filewriter = csv.writer(csv_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)

activity.to_csv(csv_filepath, index = False, header=True)






# data_files = glob.glob("./data/[0-9]/*.tcx.gz")

# for file in data_files:

#     with gzip.open(file, 'rb') as f:
#         file_content = f.read()

    # fitfile = fitparse.FitFile(file_content)

    # for message in fitfile.get_messages():
    #     if message.name == "record":
    #         print(message.get_values())
    #         print("\n")
    # print(file_content)
    # print("\n")


            























