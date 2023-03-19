import os
import pandas as pd
import happybase

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

# Generate column containing row key
def generate_rowkey(df, new_col, col1, col2):
    """
    Given a DataFrame, generates a new column by combining other two columns.

    Args:
    ----------
    df : pd.DataFrame
        DataFrame in which a new column wants to be created.
    new_col : str
        New column which will be created.
    col1 : str
        First column to be combined.
    col2 : str
        Second column to be combined.

    Returns:
    ----------
        Input DataFrame, but having added the new column specified.
    """
    df[new_col] = df[col1] + df[col2]
    return df

# Calculate average air time by route
def calculate_avg_duration(df):
    """
    Given a DataFrame, groups by the rowKey column and calculates the 
    average flight duration for each group.

    Args:
    ----------
    df : pd.DataFrame
        Input DataFrame.

    Returns:
    ----------
        Grouped DataFrame.
    """
    df_avg_duration = df.groupby(by="rowKey").agg(Avg_duration=("AirTime", "mean")).reset_index()
    return df_avg_duration

# Calculate average delay for each airline and route
def calculate_avg_delay(df):
    """
    Given a DataFrame, groups by the rowKey and UniqueCarrier columns and calculates the 
    average departure and arrival delay for each group.

    Args:
    ----------
    df : pd.DataFrame
        Input DataFrame.

    Returns:
    ----------
        Grouped DataFrame.
    """
    df_avg_delay = df.groupby(by=["rowKey", "UniqueCarrier"]).agg(
        Avg_delay_dep=("DepDelay", "mean"), 
        Avg_delay_arr=("ArrDelay", "mean")
        ).reset_index()
    return df_avg_delay

# Get airlines with higher number of routes per route
def sort_by_freq(df):
    """
    Given a DataFrame, groups by the rowKey and UniqueCarrier columns and calculates the 
    number of flights for each group. It also sorts the information by rowKey and the number 
    of flights in ascending and descending order respectively.

    Args:
    ----------
    df : pd.DataFrame
        Input DataFrame.

    Returns:
    ----------
        Grouped DataFrame.
    """
    df_n_routes = df.groupby(by=["rowKey", "UniqueCarrier"]).agg(
        N_routes=("UniqueCarrier", "count")
        ).reset_index()
    df_n_routes.sort_values(by=["rowKey", "N_routes"], ascending=[True, False], inplace=True)
    return df_n_routes

# Get most used airplane by UniqueCarrier
def get_most_used_airplane(df):
    """
    Given a DataFrame, groups by the rowKey, UniqueCarrier and TailNum columns and calculates the 
    number of flights for each group. It also sorts the information by rowKey, UniqueCarrier and number 
    of flights in ascending, ascending and descending order respectively. Finally, it groups the information 
    by rowKey and UniqueCarrier and gets the first observation, which corresponds to the airplane with more 
    fligths for that rowKey and UniqueCarrier.

    Args:
    ----------
    df : pd.DataFrame
        Input DataFrame.

    Returns:
    ----------
        Grouped DataFrame.
    """
    df_airplane = df.groupby(by=["rowKey", "UniqueCarrier", "TailNum"]).agg(
        N_routes_airplane=("UniqueCarrier", "count")
        ).reset_index()
    df_airplane.sort_values(by=["rowKey", "UniqueCarrier", "N_routes_airplane"], ascending=[True, True, False], inplace=True)
    df_airplane = df_airplane.groupby(by=["rowKey", "UniqueCarrier"]).first().reset_index()
    return df_airplane

def format_number(number : int, n_digits : int = 5):
    """
    Given a number, add the neccesary zeros at the left in order to 
    get a string with the number of digits desired.

    Args:
    ----------
    number : int
        Input number.
    n_digits : int
        Number total digits the number must have.

    Returns:
    ----------
    n : str
        Number introduces as input, with the neccesary zeros 
        at the left in order to have the desired lenght.
    """
    len_number = len(str(number))
    n = "0"*(n_digits-len_number) + str(number)
    return n

# Inserts information into HBase
def insert_df(table, df):
    """
    Inserts information from a DataFrame into an HBase table.

    Args:
    ----------
    table : 
        HBase table where the information needs to be inserted.
    df: pd.DataFrame
        DataFrame which contains the information to be inserted into HBase.

    Returns:
    ----------
        None
    """
    for row in df.itertuples():
        # Get number with binaries
        rank = format_number(row.Rank)
        # column_carrier = row.UniqueCarrier
        column_global = "99999"
        table.put(
            to_bytes(row.rowKey), 
                {
                    to_bytes(f"C:{rank}"): to_bytes(row.JSON),
                    to_bytes(f"C:{column_global}"): to_bytes(row.Route_avg_duration)
                    }
                )

# Create DataFrame with data formatted properly to be inserted into HBase
def load_df_to_hbase(table, df):
    """
    Given a DataFrame, creates two new columns (JSON and Route_avg_duration), 
    which will be used to insert information into an HBase table.

    Args:
    ----------
    table : 
        HBase table table where the information needs to be inserted.
    df: pd.DataFrame
        DataFrame with data to be inserted into HBase.
    columns: list(str)

    Returns:
    ----------
        None
    """
    count = 0
    airlines_info = []
    routes_avg_duration = []
    for row in df.itertuples():
        airline_info = {
            "UniqueCarrier": row.UniqueCarrier,
            "AvgDelayDep": row.Avg_delay_dep,
            "AvgDelayArr": row.Avg_delay_arr,
            "Nroutes": row.N_routes,
            "TailNum": row.TailNum,
            "NroutesAirplane": row.N_routes_airplane 
        }

        route_avg_duration = {
            "Route_avg_duration": row.Avg_duration
        }

        airlines_info.append(airline_info)
        routes_avg_duration.append(route_avg_duration)

    df["JSON"] = airlines_info
    df["Route_avg_duration"] = routes_avg_duration

    # Insert data into HBase
    insert_df(table=table, df=df)


###################################################################################################
# DEFINE VARIABLES
###################################################################################################

# Files with flights data
data_dir = os.path.join("/tmp", "nosql", "airData")
years = ["2007", "2008"]
file_format = ".csv"

# IP address of server where HBase is configured
ip = "192.168.80.33"
# Namespace in HBase
namespace = "airdata_425"
# Table name in HBase
table = "routes"

# Columns to be used (therefore, we load what is only neccesary)
columns = [
    "Origin",
    "Dest",
    "AirTime",
    "UniqueCarrier",
    "DepDelay",
    "ArrDelay",
    "TailNum"
]

# Get connection with specified namespace and table
routes = getConn(ip).table(f"{namespace}:{table}")

###################################################################################################
# LOAD CSV FILES
###################################################################################################

# Create empty DataFrame with the columns of interest
data = pd.DataFrame(columns=columns)

# Ierate through each file and concat it to the DataFrame already created
for year in years:
    filepath = os.path.join(data_dir, year + file_format)
    # Get aggregates
    data_year = pd.read_csv(filepath, sep=',', usecols=columns)
    data = pd.concat([data, data_year], axis=0)

###################################################################################################
# GET AGGREGATES FROM THE DATA
###################################################################################################

# Create rowKey column
df = generate_rowkey(data, "rowKey", "Origin", "Dest")
# Get Average Duration
df_avg_duration = calculate_avg_duration(df)
# Get Average Departure and Arrival Delay
df_avg_delay = calculate_avg_delay(df)
# Sorts airlines in the same route by flights in descending order
df_n_routes = sort_by_freq(df)
# Get most used airplane by each route and airline combination
df_airplane = get_most_used_airplane(df)

###################################################################################################
# GENERATE DATAFRAME BY APLLYING JOINS TO INSERT IN HBASE
###################################################################################################

# APPLY JOINS
df_final = df_avg_delay.merge(df_avg_duration, how="left", on="rowKey")
df_final = df_final.merge(df_n_routes, how="left", on=["rowKey", "UniqueCarrier"])
# This df has different number of rows (2 less)
df_final = df_final.merge(df_airplane, how="left", on=["rowKey", "UniqueCarrier"])
# Sort by N_route and get ranking by higher number of flights
df_final.sort_values(by=["rowKey", "N_routes"], ascending=[True, False], inplace=True)
df_final["Rank"] = df_final.groupby("rowKey").cumcount(ascending=True) + 1

###################################################################################################
# INSERT DATA INTO HBASE
###################################################################################################

load_df_to_hbase(table=routes, df=df_final)