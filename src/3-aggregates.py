# !pip install pandas
import os
import pandas as pd

# Generate column containing row key
def generate_rowkey(df):
    df["rowKey"] = df["Origin"] + df["Dest"]

# generate_rowkey(flights_2007)
# generate_rowkey(flights_2008)

# Group by key
def calculate_avg_duration(df):
    return df.groupby(by="rowKey").agg(avg_duration=("AirTime", "mean"))

# avg_duration_2007 = calculate_avg_duration(flights_2007)
# avg_duration_2008 = calculate_avg_duration(flights_2008)

# print(avg_duration_2007.head())
# print(avg_duration_2008.head())

# Calculate average delay for each airline and route
def calculate_avg_delay(df):
    return df.groupby(by=["rowKey", "UniqueCarrier"]).agg(avg_delay_dep=("DepDelay", "mean"), avg_delay_arr=("ArrDelay", "mean"))

def sort_by_freq(df, col):
    df_count = df.groupby(by="count")
    df_count.sort_values(by=col, ascending=False, inplace=True)
    return df_count

# avg_delay_2007 = calculate_avg_delay(flights_2007)
# avg_delay_2008 = calculate_avg_delay(flights_2008)

# print(avg_delay_2007.head())
# print(avg_delay_2008.head())

# Files with flights data
data_dir = os.path.join("/tmp", "nosql", "airData")
years = ["2007", "2008"]
file_format = ".csv"

# Get batches of size specified
batch_size = 100000

# load data into pandas
chunksize = 1000000
flights_2007 = pd.read_csv('/tmp/nosql/airData/2007.csv', sep=',', nrows=chunksize)
flights_2008 = pd.read_csv('/tmp/nosql/airData/2008.csv', sep=',', nrows=chunksize)


for year in years:
    filepath = os.path.join(data_dir, year + file_format)
    load_csv_to_hbase(table=flights, filepath=filepath, columns=columns, batch_size=batch_size)