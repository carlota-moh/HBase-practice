# !pip install pandas
import os
import pandas as pd

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

# Get airlines with higher number of flights per route
def sort_by_freq(df):
    df_n_flights = df.groupby(by=["rowKey", "UniqueCarrier"]).agg(
        N_flights=("UniqueCarrier", "count")
        ).reset_index()
    df_n_flights.sort_values(by=["rowKey", "N_flights"], ascending=[True, False], inplace=True)
    return df_n_flights

# Get most used airplane by UniqueCarrier
def get_most_used_airplane(df):
    df_airplane = df.groupby(by=["rowKey", "UniqueCarrier", "TailNum"]).agg(
        N_flights_airplane=("UniqueCarrier", "count")
        ).reset_index()
    df_airplane.sort_values(by=["rowKey", "UniqueCarrier", "N_flights_airplane"], ascending=[True, True, False], inplace=True)
    df_airplane = df_airplane.groupby(by=["rowKey", "UniqueCarrier"]).first().reset_index()
    return df_airplane

# Files with flights data
data_dir = os.path.join("/tmp", "nosql", "airData")
years = ["2007", "2008"]
file_format = ".csv"

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

for year in years:
    filepath = os.path.join(data_dir, year + file_format)

    # Get aggregates
    data = pd.read_csv(filepath, sep=',', usecols=columns)
    df = generate_rowkey(data)
    df_avg_duration = calculate_avg_duration(df)
    df_avg_delay = calculate_avg_delay(df)
    df_n_flights = sort_by_freq(df)
    df_airplane = get_most_used_airplane(df)

    # JOINS
    df_final = df_avg_delay.merge(df_avg_duration, how="left", on="rowKey")
    df_final = df_final.merge(df_n_flights, how="left", on=["rowKey", "UniqueCarrier"])
    # This df has different number of rows (2 less), and I do not know why yet 
    df_final = df_final.merge(df_airplane, how="left", on=["rowKey", "UniqueCarrier"]) 
    print(df_final.shape)
    print(df_final.head())