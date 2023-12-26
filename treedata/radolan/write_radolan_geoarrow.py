import pandas as pd
import geopandas as gpd
import pyarrow.feather as feather
from lonboard._geoarrow.geopandas_interop import geopandas_to_geoarrow


def write_geoarrow_content(path, file_name):
    df = pd.read_csv(f"{path}{file_name}.csv")
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.lng, df.lat)
    )
    table = geopandas_to_geoarrow(gdf, preserve_index=False)
    file_path = f"{path}{file_name}.feather"
    feather.write_feather(
        table, file_path, compression="uncompressed"
    )


def write_radolan_geoarrow(path):
    file_name = "trees"
    write_geoarrow_content(path, file_name)
