# /opt/miniconda3/bin/conda init
# conda activate nosql_mbd25
#!pip install happybase

import happybase
import csv
def getConn():
    return happybase.Connection(host='192.168.80.33')

#funcion auxiliar para convertir a bytes lo que queremos almacenar en HBase
def to_bytes(value):
  return bytes(str(value), 'utf-8')

flights = getConn().table('airdata_425:flights')

flights_2007 = '/tmp/nosql/airData/2007.csv'
flights_2008 = '/tmp/nosql/airData/2008.csv'

flights_data = [flights_2007, flights_2008]

for flight_path in flights_data:
    print("loading %s into airports" % flight_path)
    with open(flight_path, 'r') as f:
        reader = csv.DictReader(f)
        i = 0
        for row in reader:
            # design rowkey:
            if len(row['Month']) == 1:
                row['Month'] = str(0)+row['Month']
            
            if len(row['DayofMonth']) == 1:
                row['DayofMonth'] = str(0)+row['DayofMonth']

            rowkey = row['Year']+row['Month']+row['DayofMonth']+row['FlightNum']+row['Origin']+row['Dest']
            # hora de salida, hora de llegada, número de vuelo, origen, destino, número de aeronave y distancia recorrida.
            i += 1
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
            
    print("loaded airports with %d entries" % i)