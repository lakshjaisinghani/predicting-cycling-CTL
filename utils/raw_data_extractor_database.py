import glob
import gzip
import csv
import fitparse
import os
import time

# MySql connector
import mysql.connector
from mysql.connector import Error

# Global file path
GLOBAL_FILEPATH = os.path.dirname(os.path.dirname(__file__))
CSV_DIR = os.path.join(GLOBAL_FILEPATH, "csv_data") 
FILENAME = ""

is_garmin = False
is_cycling = True
sport = ""
data = {}
data_lst = []
want_csv = False

def create_connection(host_name, user_name, user_password, database):
  ''' This function connects to a database an/or 
  prints errors if there are any connection issues.
  '''
  connection = None
  
  try:
      connection = mysql.connector.connect(
          host=host_name,
          user=user_name,
          passwd=user_password,
          database=database
      )
      print("Connection to MySQL DB successful")
  except Error as e:
      print(f"The error '{e}' occurred")

  return connection

def send_query(connection, query):
  ''' This function that sends an SQL query to the 
  connected database.
  '''
  cursor = connection.cursor()
  try:
      cursor.execute(query)
      print("Query excecuted successfully")
  except Error as e:
      print(f"The error '{e}' occurred")


def parse_record(values):
    ''' This function parses the contents of a message named records
    into a dictionory and returns it.
    '''

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
                elif key == "timestamp":
                    # convert from datetime object to str 
                    data['date'] = str(message.get_values()['timestamp'])[:10]
                    data['time'] = str(message.get_values()['timestamp'])[11:]
                else:
                    data[key] = values[key]

            except:
                data[key] = float("NaN")
    return data

if __name__ == "__main__":

    # connect to database
    connection = create_connection("localhost", "root", "lemoncake", "cycling_data")

    data_files = glob.glob("../data/[0-9]/*.fit.gz")
    # For testing purpopses
    
    print("Cleaning Data has begun !!")
    start_time = time.time()

    for file in data_files:

        print("Processing : " + file)

        with gzip.open(file, 'rb') as f:
            file_content = f.read()

        fitfile = fitparse.FitFile(file_content)

        for message in fitfile.get_messages():

            if message.name == "file_id":
                manufacturer = message.get_values()['manufacturer']
                
                # convert from datetime object to str 
                date = str(message.get_values()['time_created'])[:10]

                # append date
                query = "INSERT INTO Rides (date) VALUES ('" + date + "');"
                send_query(connection, query)
                connection.commit()

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
                data_lst.append(data)


    end_time = time.time()
    print("Completed data wrangling. \n")
    print("Time Taken: " + str(end_time-start_time))

    