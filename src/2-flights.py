import os
import happybase
import pandas as pd
import numpy as np

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
def insert_batch(table, df, batch_size):
    """
    Inserts information from a DataFrame into an HBase table.

    Args:
    ----------
    table : 
        HBase table table where the information needs to be inserted.
    df: pd.DataFrame
        DataFrame which contains the information to be inserted into HBase.
    batch_size: int
        Number of rows that should be inserted into the HBase table at once as a batch operation.

    Returns:
    ----------
        None
    """
    with table.batch(batch_size=batch_size) as batch:
        for row in df.itertuples():
            column_name = row.UniqueCarrier + str(row.FlightNum) + row.Origin + row.Dest
            batch.put(
                to_bytes(row.rowkey), 
                    {
                        to_bytes(f"F:{column_name}"): to_bytes(row.JSON),
                        }
                    )

# Create DataFrame with data formatted properly to be inserted into HBase
def load_csv_to_hbase(table, filepath, columns, batch_size):
    """
    Loads CSV in batches as a DataFrame. Then creates two new columns (Year and JSON), 
    which will be used to insert information into an HBase table.

    Args:
    ----------
    table : 
        HBase table where the information needs to be inserted.
    filepath: str
        filepath where the CSV file is stored.
    columns: list(str)
        List of columns to load in df.
    batch_size: int
        Number of rows that should be inserted into the HBase table at once as a batch operation.

    Returns:
    ----------
        None
    """
    # Starts at 0
    count = 0
    # Get iterator
    df_iterator = pd.read_csv(filepath, usecols=columns, chunksize=batch_size)
    for df in df_iterator:
        # Columns formatting
        # for col in ["Month", "DayofMonth"]:
        #     df[col] = df[col].astype(str)
        #     df[col] = np.where(len(df[col])==1, str(0)+df[col], df[col])

        for col in ["Month", "DayofMonth"]:
            df[col] = df[col].astype(str)
            df[col] = [str(0)+val if len(val) == 1 else val for val in df[col]]
        
        # Create column for RowKey
        df['Year'] = df['Year'].astype(str)
        df["rowkey"] = df['Year'] + df['Month'] + df['DayofMonth']

        # Create column in JSON format with all the required information
        json_list = []
        for row in df.itertuples():
            json = {
                "DepTime": row.DepTime,
                "ArrTime": row.ArrTime,
                "FlightNum": row.FlightNum,
                "Origin": row.Origin,
                "Dest": row.Dest,
                "TailNum": row.TailNum,
                "Distance": row.Distance 
            }
            json_list.append(json)
        df["JSON"] = json_list

        # Console message per n rows iterated
        count += len(df)
        if (count % 10**6) == 0:
            print(f"{count}")

        # Insert data into HBase
        insert_batch(table=table, df=df, batch_size=batch_size)

###################################################################################################
# DEFINE VARIABLES
###################################################################################################

# IP address of server where HBase is configured
ip = "192.168.80.33"
# Namespace in HBase
namespace = "airdata_425"
# Table name in HBase
table = "flights"

# Get connection with specified namespace and table
flights = getConn(ip).table(f"{namespace}:{table}")

# Files with flights data
data_dir = os.path.join("/tmp", "nosql", "airData")
years = ["2007", "2008"]
file_format = ".csv"

# Get batches of size specified
batch_size = 100000

# Columns of interest
columns = [
    "Year",
    "Month",
    "DayofMonth",
    "UniqueCarrier",
    "FlightNum",
    "Origin",
    "Dest",
    "DepTime",
    "ArrTime",
    "TailNum",
    "Distance"
    ]

###################################################################################################
# LOAD CSV FILES AND INSERT DATA INTO HBASE
###################################################################################################

# Iterate through each file and insert information into HBase
for year in years:
    filepath = os.path.join(data_dir, year + file_format)
    load_csv_to_hbase(table=flights, filepath=filepath, columns=columns, batch_size=batch_size)