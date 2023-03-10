# /opt/miniconda3/bin/conda init
# conda activate nosql_mbd25
#!pip install happybase

import happybase
import csv
def getConn():
    return happybase.Connection(host='192.168.80.33')

#Si al conectar da el error -> thriftpy2.transport.base.TTransportException: TTransportException(type=1, message="Could not connect to ('0.0.0.0', 9090)")
#ejecutar en una terminal: hbase thrift start -p 9090

#funcion auxiliar para convertir a bytes lo que queremos almacenar en HBase
def to_bytes(value):
  return bytes(str(value), 'utf-8')

airports = getConn().table('airdata_425:airports')

airports_path = '/tmp/nosql/airData/airports.csv'

print("loading %s into airports" % airports_path)

with open(airports_path, 'r') as f:
    reader = csv.DictReader(f)
    i = 0
    for row in reader:
        i += 1
        airports.put(to_bytes(row['iata']),
            {
            to_bytes('info:airport'): to_bytes(row['airport']),
            to_bytes('info:city'): to_bytes(row['city']),
            to_bytes('info:state'): to_bytes(row['state']),
            to_bytes('info:long'): to_bytes(row['long']),
            to_bytes('info:country'): to_bytes(row['country']),
            to_bytes('info:lat'): to_bytes(row['lat']),
    })
        
print("loaded airports with %d entries" % i)