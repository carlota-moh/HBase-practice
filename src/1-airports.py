import os
import happybase
import pandas as pd
import csv

# Establish connection with HBase
def getConn(ip):
    """
    Establish a connection with HBase using happybase library.

    Args:
    ----------
    ip : str
        ip address a connection needs to be established with.

    Returns:
    ----------
        Connection with HBase cluster in the IP address specified.
    """
    return happybase.Connection(host=ip)

# Function to convert to bytes the data which will be stored in HBase
def to_bytes(value):
    """
    Given a specific value, returns it converted 
    to bytes using UTF-8 encoding.

    Args:
    ----------
    value : any
        Any Python object which must be converted to bytes.

    Returns:
    ----------
        Value introduced as input, but converted to bytes.
    """
    return bytes(str(value), "utf-8")

# Inserts information into HBase
def insert_df(table, row):
    """
    Inserts information from a row into an HBase table.

    Args:
    ----------
    table : 
        HBase table where the information needs to be inserted.
    row: dict
        Dictionary where keys are the column names and values are 
        the values of those column for that specify row.

    Returns:
    ----------
        None
    """
    table.put(
        to_bytes(row["iata"]), 
            {
            to_bytes("I:airport"): to_bytes(row["airport"].strip()),
            to_bytes("I:city"): to_bytes(row["city"].strip()),
            to_bytes("I:state"): to_bytes(row["state"].strip()),
            to_bytes("I:long"): to_bytes(row["long"].strip()),
            to_bytes("I:country"): to_bytes(row["country"].strip()),
            to_bytes("I:lat"): to_bytes(row["lat"].strip()),
            }
            )

###################################################################################################
# DEFINE VARIABLES
###################################################################################################

# IP address of server where HBase is configured
ip = "192.168.80.33"
# Namespace in HBase
namespace = "airdata_425"
# Table name in HBase
table = "airports"

# Get connection with specified namespace and table
airports = getConn(ip).table(f"{namespace}:{table}")

# File with airports data
data_dir = os.path.join("/tmp", "nosql", "airData")
filename = "airports.csv"
filepath = os.path.join(data_dir, filename)

# Check with Pandas which rows are now properly formatted
# df = pd.read_csv(filepath, sep=',', error_bad_lines=False)
# print(df.shape)

problematic_rows = [
    303,
    488,
    1013,
    1776,
    2378,
    2696,
    2758,
    2822,
    3122
]

###################################################################################################
# LOAD DATA AND INSERT IT INTO HBASE
###################################################################################################

with open(filepath, "r") as f:
    reader = csv.DictReader(f)
    # Start at two because problemati columns is the position of the row in the csv file, 
    # which starts from 1, being the header the 1st column
    i = 2
    for row in reader:
        if i not in problematic_rows:
            pass
        elif i == 303:
            row = {'iata': '35A', 'airport': 'Union County', 'city': 'Troy Shelton, Union', 'state': 'SC', 'country': 'USA', 
                   'lat': '34.68680111', 'long': '-81.64121167'}
        elif i == 488:
            row = {'iata': '53A', 'airport': 'Dr. C.P. Savage Sr.', 'city': 'Montezuma', 'state': 'GA', 'country': 'USA', 
                   'lat': '32.302', 'long': '-84.00747222'}
        elif i == 1013:
            row = {'iata': 'BTR', 'airport': 'Baton Rouge Metropolitan, Ryan', 'city': 'Baton Rouge', 'state': 'LA', 'country': 'USA', 
                   'lat': '30.53316083', 'long': '-91.14963444'}
        elif i == 1776:
            row = {'iata': 'HTW', 'airport': 'Lawrence County Airpark, Inc', 'city': 'Chesapeake', 'state': 'OH', 'country': 'USA', 
                   'lat': '38.41924861', 'long': '-82.4943225'}
        elif i == 2378:
            row = {'iata': 'N25', 'airport': 'Westport', 'city': 'Westport NY', 'state': 'NY', 'country': 'USA', 
                   'lat': '44.15838611', 'long': '-73.43290444'}
        elif i == 2696:
            row = {'iata': 'PUW', 'airport': 'Pullman/Moscow Regional', 'city': 'Pullman/Moscow', 'state': 'WA/ID', 'country': 'USA', 
                   'lat': '46.74386111', 'long': '-117.1095833'}
        elif i == 2758:
            row = {'iata': 'RDG', 'airport': 'Reading Muni - Gen Carl A Spaatz', 'city': 'Reading', 'state': 'PA', 'country': 'USA', 
                   'lat': '40.3785', 'long': '-75.96525'}
        elif i == 2822:
            row = {'iata': 'RVS', 'airport': 'Richard Lloyd Jones Jr.', 'city': 'Tulsa', 'state': 'OK', 'country': 'USA', 
                   'lat': '36.0396275', 'long': '-95.984635'}
        elif i == 3122:
            row = {'iata': 'TOC', 'airport': 'Toccoa - R G Le Tourneau', 'city': 'Toccoa', 'state': 'GA', 'country': 'USA', 
                   'lat': '34.59376444', 'long': '-83.2958'}
        i+=1
        insert_df(airports, row)