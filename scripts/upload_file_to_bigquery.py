"""Upload the data contained in a csv file into bigquery."""

import argparse
import sys

import geopandas
from google.cloud import bigquery
import pandas
import shapely.wkt


expected_columns = {"id", "geometry", "epsg"}


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


def read_geodataframe_from_csv(file_path: str) -> geopandas.GeoDataFrame:
    """Read a DataFrame with pandas, then convert its geometry field to a shapely object.
    Ensures that geometric fields are valid before uploading them."""
    df = pandas.read_csv(file_path)
    assert set(df.columns) == expected_columns, \
        f"columns in the file are {df.columns}, they must be {expected_columns}"
    df = geopandas.GeoDataFrame(df, geometry=df['geometry'].apply(shapely.wkt.loads))
    return df


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
