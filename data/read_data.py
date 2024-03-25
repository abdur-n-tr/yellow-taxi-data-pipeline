import pandas as pd
import pyarrow.parquet as pq

# # Specify the path to the Parquet file
# parquet_file_path = 'yellow_tripdata_2023-10.parquet'

# # Read the Parquet file into a PyArrow Table
# table = pq.read_table(parquet_file_path)

# # Convert the PyArrow Table to a pandas DataFrame if needed
# df = table.to_pandas()

# # Now you can work with the DataFrame
# # print(df['payment_type'].unique(), df['DOLocationID'].nunique())

# print(df['passenger_count'].value_counts(normalize=False))

# print(df.head()['passenger_count'])

df = pd.read_csv("./rides_data/rides_2023-10-01.csv")

print(df['RatecodeID'].isnull().sum())
print(df['RatecodeID'].value_counts())