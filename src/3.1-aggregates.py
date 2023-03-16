# !pip install pandas
import os
import pandas as pd
import happybase

# Establish connection with HBase
def getConn(ip):
    return happybase.Connection(host=ip)

# Function to convert to bytes the data which will be stored in HBase
def to_bytes(value):
  return bytes(str(value), "utf-8")

# Generate column containing row key
def generate_rowkey(df):
    df["rowKey"] = df["Origin"] + df["Dest"]
    return df

# Calculate average air time by route
def calculate_avg_duration(df):
    df_avg_duration = df.groupby(by="rowKey").agg(Avg_duration=("AirTime", "mean")).reset_index()
    return df_avg_duration

# Calculate average delay for each airline and route
def calculate_avg_delay(df):
    df_avg_delay = df.groupby(by=["rowKey", "UniqueCarrier"]).agg(
        Avg_delay_dep=("DepDelay", "mean"), 
        Avg_delay_arr=("ArrDelay", "mean")
        ).reset_index()
    return df_avg_delay

# Get airlines with higher number of routes per route
def sort_by_freq(df):
    df_n_routes = df.groupby(by=["rowKey", "UniqueCarrier"]).agg(
        N_routes=("UniqueCarrier", "count")
        ).reset_index()
    df_n_routes.sort_values(by=["rowKey", "N_routes"], ascending=[True, False], inplace=True)
    return df_n_routes

# Get most used airplane by UniqueCarrier
def get_most_used_airplane(df):
    df_airplane = df.groupby(by=["rowKey", "UniqueCarrier", "TailNum"]).agg(
        N_routes_airplane=("UniqueCarrier", "count")
        ).reset_index()
    df_airplane.sort_values(by=["rowKey", "UniqueCarrier", "N_routes_airplane"], ascending=[True, True, False], inplace=True)
    df_airplane = df_airplane.groupby(by=["rowKey", "UniqueCarrier"]).first().reset_index()
    return df_airplane

def insert_df(table, df):
    for row in df.itertuples():
        column_carrier = row.UniqueCarrier
        column_global = "AvgRouteDuration"
        table.put(
            to_bytes(row.rowkey), 
                {
                    to_bytes(f"carrier:{column_carrier}"): to_bytes(row.JSON),
                    to_bytes(f"route:{column_global}"): to_bytes(row.Avg_duration)
                    }
                )

def load_df_to_hbase(table, df):
    count = 0
    json_list = []
    for row in df.itertuples():
        json = {
            "AvgDelayDep": row.Avg_delay_dep,
            "AvgDelayArr": row.Avg_delay_arr,
            "FlightNum": row.FlightNum,
            "Nroutes": row.Dest,
            "TailNum": row.TailNum,
            "NroutesAirplane": row.N_routes_airplane 
        }
        json_list.append(json)

    df["JSON"] = json_list
    # Console message
    count += len(df)

    if (count % 1000) == 0:
        print(f"{count}")

    # Insert data into HBase
    insert_df(table=table, df=df)

# Files with routes data
data_dir = os.path.join("/tmp", "nosql", "airData")
years = ["2007", "2008"]
file_format = ".csv"

# IP address of server where HBase is configured
ip = "192.168.80.33"
# Namespace in HBase
namespace = "airdata_425"
# Table name in HBase
table = "routes"

# Get batches of size specified
batch_size = 100000

columns = [
    "Origin",
    "Dest",
    "AirTime",
    "UniqueCarrier",
    "DepDelay",
    "ArrDelay",
    "TailNum"
]

###########################################################################

# Get connection with specified namespace and table
routes = getConn(ip).table(f"{namespace}:{table}")

for year in years:
    filepath = os.path.join(data_dir, year + file_format)

    # Get aggregates
    data = pd.read_csv(filepath, sep=',', usecols=columns)
    df = generate_rowkey(data)
    df_avg_duration = calculate_avg_duration(df)
    df_avg_delay = calculate_avg_delay(df)
    df_n_routes = sort_by_freq(df)
    df_airplane = get_most_used_airplane(df)

    # JOINS
    df_final = df_avg_delay.merge(df_avg_duration, how="left", on="rowKey")
    df_final = df_final.merge(df_n_routes, how="left", on=["rowKey", "UniqueCarrier"])
    # This df has different number of rows (2 less), and I do not know why yet 
    df_final = df_final.merge(df_airplane, how="left", on=["rowKey", "UniqueCarrier"]) 
    print(df_final.shape)
    print(df_final.head())
    load_df_to_hbase(table=routes, df=df_final)