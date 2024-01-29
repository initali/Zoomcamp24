import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os

# get_ipython().system('wget  https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz')


def main(params):
    csv_file = "output.csv.gz"
    os.system(f"wget {params.url} -O {csv_file}")

    engine = create_engine(
        f"postgresql://{params.user}:{params.password}@{params.host}:{params.port}/{params.db}"
    )
    engine.connect()

    data_config = {
        "filepath_or_buffer": csv_file,
        "parse_dates": ["tpep_pickup_datetime", "tpep_dropoff_datetime"],
    }
    engine_config = {
        "name": params.table_name,
        "con": engine,
    }

    df_iter = pd.read_csv(
        **data_config,
        iterator=True,
        chunksize=100000,
    )
    df = next(df_iter)
    df.head(n=0).to_sql(**engine_config, if_exists="replace")

    while True:
        t_start = time()
        df.to_sql(**engine_config, if_exists="append")
        t_end = time()
        print(f"inserted another chunk..., took {t_end - t_start:.3f} seconds")
        df = next(df_iter)

    query = """
    select count(*) from yellow_taxi_data;
    """
    pd.read_sql(query, con=engine)


# get_ipython().system('jupyter nbconvert --to=script upload-data.ipynb')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to postgres")

    parser.add_argument("--user", help="postgres user name")
    parser.add_argument("--password", help="postgres user password")
    parser.add_argument("--host", help="postgres host name or url")
    parser.add_argument("--port", help="postgres host port")
    parser.add_argument("--db", help="database to connect")
    parser.add_argument("--table_name", help="name of the output table")
    parser.add_argument("--url", help="csv file url")

    args = parser.parse_args()

    main(args)
