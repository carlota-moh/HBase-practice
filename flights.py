# /opt/miniconda3/bin/conda init
# conda activate nosql_mbd25
# !pip install happybase

# ======================================================================================================================================
# INSERTING DATA ROW BY ROW
# ======================================================================================================================================

import happybase
import csv

def getConn():
    return happybase.Connection(host='192.168.80.33')

# Function to convert to bytes the data which will be stored in HBase
def to_bytes(value):
  return bytes(str(value), 'utf-8')

# Get connection with specified namespace and table
flights = getConn().table('airdata_425:flights')

# Ficheros con datos sobre vuelos
flights_2007 = '/tmp/nosql/airData/2007.csv'
flights_2008 = '/tmp/nosql/airData/2008.csv'

flights_data = [flights_2007, flights_2008]

for flight_path in flights_data:
    print("loading %s into flights" % flight_path)
    with open(flight_path, 'r') as f:
        reader = csv.DictReader(f)
        i = 0
        for row in reader:
            # All months must have length 2
            if len(row['Month']) == 1:
                row['Month'] = str(0) + row['Month']
            # All days must have length 2
            if len(row['DayofMonth']) == 1:
                row['DayofMonth'] = str(0) + row['DayofMonth']
            # Get rowkey
            rowkey = row['Year'] + row['Month'] + row['DayofMonth'] + row['UniqueCarrier'] + row['FlightNum'] + row['Origin'] + row['Dest']
            # Insert data into HBase
            flights.put(to_bytes(rowkey),
                {
                to_bytes('info:DepTime'): to_bytes(row['DepTime']),
                to_bytes('info:ArrTime'): to_bytes(row['ArrTime']),
                to_bytes('info:FlightNum'): to_bytes(row['FlightNum']),
                to_bytes('info:Origin'): to_bytes(row['Origin']),
                to_bytes('info:Dest'): to_bytes(row['Dest']),
                to_bytes('info:TailNum'): to_bytes(row['TailNum']),
                to_bytes('info:Distance'): to_bytes(row['Distance'])
            })

            i += 1
            # Keep track of execution evolution in console
            if i % 1000 == 0:
                print("Loaded %d entries" % i)
        
    print("Loaded {} with {} entries".format(flight_path, i))


# ======================================================================================================================================
# INSERTING DATA IN BATCHES INSTEAD OF GOING ONE BY ONE
# ======================================================================================================================================

import happybase
import csv

def getConn():
    return happybase.Connection(host='192.168.80.33')

# Function to convert to bytes the data which will be stored in HBase
def to_bytes(value):
  return bytes(str(value), 'utf-8')

# Get connection with specified namespace and table
flights = getConn().table('airdata_425:flights')

# Ficheros con datos sobre vuelos
flights_2007 = '/tmp/nosql/airData/2007.csv'
flights_2008 = '/tmp/nosql/airData/2008.csv'

batch_size = 100000

# Function to insert a batch of rows into HBase
def insert_batch(table, rows):
    with table.batch(batch_size=batch_size) as batch:
        for row in rows:
            rowkey = row['Year'] + row['Month'] + row['DayofMonth'] + row['UniqueCarrier'] + row['FlightNum'] + row['Origin'] + row['Dest']
            batch.put(to_bytes(rowkey), {
                to_bytes('info:DepTime'): to_bytes(row['DepTime']),
                to_bytes('info:ArrTime'): to_bytes(row['ArrTime']),
                to_bytes('info:FlightNum'): to_bytes(row['FlightNum']),
                to_bytes('info:Origin'): to_bytes(row['Origin']),
                to_bytes('info:Dest'): to_bytes(row['Dest']),
                to_bytes('info:TailNum'): to_bytes(row['TailNum']),
                to_bytes('info:Distance'): to_bytes(row['Distance'])
            })

# Function to load a CSV file into HBase using multithreading
def load_csv_to_hbase(filename):
    print("Loading %s into flights" % filename)
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        rows = []
        i = 0
        for row in reader:
            # All months must have length 2
            if len(row['Month']) == 1:
                row['Month'] = str(0) + row['Month']
            # All days must have length 2
            if len(row['DayofMonth']) == 1:
                row['DayofMonth'] = str(0) + row['DayofMonth']
            rows.append(row)
            i += 1
            # Insert rows in batches
            if i % batch_size == 0:
                insert_batch(flights, rows)
                rows = []
                print("Loaded %d entries" % i)

        if rows:
            insert_batch(flights, rows)
            print("Loaded %d entries" % i)

    print("Loaded {} with {} entries".format(filename, i))

load_csv_to_hbase(flights_2007)
load_csv_to_hbase(flights_2008)