import pyarrow.parquet as pq
from datetime import datetime, timedelta

start_date = datetime.strptime("2023-10-01 00:00:00", "%Y-%m-%d %H:%M:%S")

parquet_file_path = 'yellow_tripdata_2023-10.parquet'
table = pq.read_table(parquet_file_path)
df = table.to_pandas()

def generate_rides_day_data():
    global start_date
    for i in range(30):
        end_date = start_date + timedelta(days=1)
        ride_day_data = df[(df['tpep_pickup_datetime'] >= start_date) & (df['tpep_pickup_datetime'] < end_date)]
        ride_file_name = f"rides_{start_date.date()}.csv"
        ride_day_data.to_csv(f"rides_data/{ride_file_name}", index=False)
        print(f"Write rides data for date: {start_date.date()} successfully!")
        start_date = end_date

if __name__ == "__main__":
    generate_rides_day_data()

    