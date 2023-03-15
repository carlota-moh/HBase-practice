# !pip install pandas
import pandas as pd

# load data into pandas
chunksize = 1000000
flights_2007 = pd.read_csv('/tmp/nosql/airData/2007.csv', sep=',', nrows=chunksize)
flights_2008 = pd.read_csv('/tmp/nosql/airData/2008.csv', sep=',', nrows=chunksize)

# generate column containing with row key
def generate_rowkey(df):
    df['rowKey'] = df['Origin']+df['Dest']

generate_rowkey(flights_2007)
generate_rowkey(flights_2008)

# group by key
def calculate_avg_duration(df):
    return df.groupby(by='rowKey').agg(avg_duration=('AirTime', 'mean'))

avg_duration_2007 = calculate_avg_duration(flights_2007)
avg_duration_2008 = calculate_avg_duration(flights_2008)

print(avg_duration_2007.head())
print(avg_duration_2008.head())

# calculate average delay for each airline and route
def calculate_avg_delay(df):
    return df.groupby(by=['rowKey', 'UniqueCarrier']).agg(avg_delay_dep=('DepDelay', 'mean'), avg_delay_arr=('ArrDelay', 'mean'))

avg_delay_2007 = calculate_avg_delay(flights_2007)
avg_delay_2008 = calculate_avg_delay(flights_2008)

print(avg_delay_2007.head())
print(avg_delay_2008.head())
