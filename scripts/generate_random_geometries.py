"""Generate random geometries in the EPSG 4326 system.
Points will be generated at random between -180 and 180 for both coordinates,
while polygons will need to use an original "imitation" base:
generated polygons will be random transformation of the existing polygons."""

import argparse

import geopandas
from numpy import random as npr
import shapely

from utils import read_geodataframe_from_csv

npr.seed(42)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('data_type', choices=["trees", "buildings"],
                        help="Will build points for trees and multipolygons for buildings")
    parser.add_argument('n_rows', type=int, help="how many items to generate.")
    parser.add_argument('save_file', type=str, help="path where to save the output file.")
    parser.add_argument('--imitation_file_path', '-i', type=str, help="Path to the imitation file for polygons")
    return parser.parse_args()


def generate_random_points(number=1000):
    random_coordinates = npr.uniform((-180, -90), (180, 90), (number, 2))
    random_points = [shapely.geometry.Point(coord) for coord in random_coordinates]
    random_series = geopandas.GeoSeries(random_points)
    random_ids = npr.uniform(0, 1e9, number).astype(int)
    random_dataframe = geopandas.GeoDataFrame({'id': random_ids, "geometry": random_series, "epsg": 4326})
    return random_dataframe


def random_transformation(geometry, transformation=shapely.affinity.skew):
    return transformation(geometry, npr.uniform(-180, 180))


def generate_random_multipolygons(imitation_df, number=1000):
    sample_df = imitation_df.sample(number, replace=True)

    # random rotation
    print("Randomly rotating...")
    sample_df['geometry'] = sample_df['geometry'].apply(random_transformation, transformation=shapely.affinity.rotate)

    # random skew
    print("Randomly skewing...")
    sample_df['geometry'] = sample_df['geometry'].apply(random_transformation, transformation=shapely.affinity.skew)

    # random shift
    print("Randomly shifting !")
    sample_df['geometry'] = sample_df['geometry'].apply(random_transformation, transformation=shapely.affinity.translate)

    # replace ids with random numbers
    sample_df['id'] = npr.uniform(1, 1e9, number).astype(int)
    sample_df.reset_index(inplace=True)
    return sample_df


if __name__ == "__main__":
    args = parse_args()
    if args.data_type == "trees":
        df = generate_random_points(args.n_rows)
    elif args.data_type == "buildings":
        if args.imitation_file_path is None:
            raise ValueError('An imitation file should be provided.')
        imitation_df = read_geodataframe_from_csv(args.imitation_file_path)
        df = generate_random_multipolygons(imitation_df, number=args.n_rows)
    df.to_csv(args.save_file, index=False)
