"""Upload the data contained in a csv file into bigquery."""

import argparse

from google.cloud import bigquery
import pandas

from utils import read_geodataframe_from_csv


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('data_type', choices=["trees", "buildings"],
                        help="what type of data is contained withing the file. Either 'trees' or 'buildings'.")
    parser.add_argument('file_paths', nargs='+', help="path to the csv file to upload.")
    return parser.parse_args()


def get_table_schema(table_name):
    client = bigquery.Client()
    schema = client.get_table(table_name).schema
    json_schema = [
        {
            "name": field.name,
            "type": field.field_type,
            "mode": field.mode
         } for field in schema
    ]
    return json_schema


def main(file_paths: str, data_type: str):
    for path in file_paths:
        df = read_geodataframe_from_csv(path)
        df['ingested_at'] = pandas.Timestamp.utcnow()  # timestamp is UTC to avoid execution to be location dependent
        table_name = f"geodata_raw.{data_type}"
        table_schema = get_table_schema(table_name)
        print(f"inserting {len(df)} rows in {table_name}")
        df.to_gbq(table_name, if_exists="append", table_schema=table_schema)


if __name__ == "__main__":
    args = parse_args()
    main(args.file_paths, args.data_type)
