import pyarrow.parquet as pq

parquet_file_path = 'yellow_tripdata_2023-10.parquet'
table = pq.read_table(parquet_file_path)
df = table.to_pandas()


rate_code_type = {
    1:"Standard rate",
    2:"JFK",
    3:"Newark",
    4:"Nassau or Westchester",
    5:"Negotiated fare",
    6:"Group ride"
}

payment_type_name = {
    1:"Credit card",
    2:"Cash",
    3:"No charge",
    4:"Dispute",
    0:"Unknown",
    6:"Voided trip"
}

def generate_rate_code_dim_data():
    rate_code_dim = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
    rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
    rate_code_dim = rate_code_dim.rename(columns={'RatecodeID': 'rate_code_id'})
    rate_code_dim = rate_code_dim[['rate_code_id', 'rate_code_name']]
    rate_code_dim = rate_code_dim.dropna()
    rate_code_dim['rate_code_id'] = rate_code_dim['rate_code_id'].astype(int)
    
    rate_code_dim.to_csv("dim_data/rate_code_dim.csv", index=False)
    print("Rate Code Dim data written successfully!")


def generate_payment_type_dim_data():
    payment_type_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    payment_type_dim = payment_type_dim.rename(columns={'payment_type': 'payment_type_id'})
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type_id'].map(payment_type_name)
    payment_type_dim = payment_type_dim[['payment_type_id', 'payment_type_name']]
    payment_type_dim = payment_type_dim.dropna()

    payment_type_dim.to_csv("dim_data/payment_type_dim.csv", index=False)
    print("Payment Type Dim data written successfully!")


if __name__ == "__main__":
    generate_rate_code_dim_data()
    generate_payment_type_dim_data()
