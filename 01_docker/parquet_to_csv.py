import pandas as pd


# Read Parquet file
df = pd.read_parquet('data/yellow_tripdata_2021-01.parquet')

# Convert to CSV
df.to_csv('data/yellow_tripdata_2021-01.csv', index=False)