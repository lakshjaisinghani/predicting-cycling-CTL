import mysql.connector
from mysql.connector import Error

# after creating a database
# connect to it
def create_connection(host_name, user_name, user_password, database):
  
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
  ''' Function that sends an SQL query to connected database
  '''
  cursor = connection.cursor()
  try:
      cursor.execute(query)
      print("Query excecuted successfully")
  except Error as e:
      print(f"The error '{e}' occurred")

#  connect
connection = create_connection("localhost", "root", "lemoncake", "cycling_data")

# create cursor
cursor = connection.cursor()

# create primary table
cursor.execute("CREATE TABLE Rides (id INT AUTO_INCREMENT PRIMARY KEY, date VARCHAR(10))")


# Queries:
# --------
# 1. create a table
# mycursor.execute("CREATE TABLE customers (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), address VARCHAR(255))")

# 2. insert into table
# sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
# val = [
#   ('Peter', 'Lowstreet 4'),
#   ('Amy', 'Apple st 652'),
#   ('Hannah', 'Mountain 21'),
#   ('Michael', 'Valley 345'),
#   ('Sandy', 'Ocean blvd 2'),
#   ('Betty', 'Green Grass 1'),
#   ('Richard', 'Sky st 331'),
#   ('Susan', 'One way 98'),
#   ('Vicky', 'Yellow Garden 2'),
#   ('Ben', 'Park Lane 38'),
#   ('William', 'Central st 954'),
#   ('Chuck', 'Main Road 989'),
#   ('Viola', 'Sideway 1633')
# ]

# mycursor.executemany(sql, val)

# mydb.commit()














