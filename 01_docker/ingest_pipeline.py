import pandas as pd
from sqlalchemy import create_engine

def ingest_data(user, password, host, port, db, table_name, csv_file):
    # Create a connection
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    # Read Data in Chunks
    first_chunk = True
    for chunk in pd.read_csv(csv_file, chunksize=100_000):
        # Process Data
        chunk['tpep_pickup_datetime'] = pd.to_datetime(chunk.tpep_pickup_datetime)
        chunk['tpep_dropoff_datetime'] = pd.to_datetime(chunk.tpep_dropoff_datetime)

        # Create Table
        chunk.to_sql(name=table_name, con=engine, if_exists="replace" if first_chunk else "append", index=False )

        print(f"Inserted a chunk of {len(chunk)} into Postgres")
        first_chunk = False

if __name__ == "__main__":
    user = "root"
    password = "password"
    host = "localhost"
    port = 5432
    db = "ny_taxi_db"
    table_name = "yellow_taxi_data"
    csv_file = "data/yellow_tripdata_2021-01.csv"

    ingest_data(user, password, host, port, db, table_name, csv_file)