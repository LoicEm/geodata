import geopandas
import pandas
import shapely.wkt

expected_columns = {"id", "geometry", "epsg"}


def read_geodataframe_from_csv(file_path: str) -> geopandas.GeoDataFrame:
    """Read a DataFrame with pandas, then convert its geometry field to a shapely object.
    Ensures that geometric fields are valid before uploading them."""
    df = pandas.read_csv(file_path)
    assert set(df.columns) == expected_columns, \
        f"columns in the file are {df.columns}, they must be {expected_columns}"
    df = geopandas.GeoDataFrame(df, geometry=df['geometry'].apply(shapely.wkt.loads))
    return df
