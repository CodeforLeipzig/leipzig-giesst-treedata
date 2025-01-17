import pandas as pd
import geopandas as gpd
import pyarrow.feather as feather
from lonboard._geoarrow.geopandas_interop import geopandas_to_geoarrow
import pyarrow.parquet as pq


def write_geoarrow_content(path, file_name):
    dtype_dic = {
        'id': str,
        'lng': float,
        'lat': float,
        'radolan_sum': int,
        'age': int
    }
    df = pd.read_csv(f"{path}{file_name}.csv", dtype=dtype_dic)
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.lng, df.lat)
    )
    gdf = gdf.drop(columns=["lng", "lat"])
    table = geopandas_to_geoarrow(gdf, preserve_index=False)

    file_path = f"{path}{file_name}.feather"
    feather.write_feather(
        table, file_path, compression="uncompressed"
    )
    pq.write_table(
        table,
        f"{path}{file_name}.parquet",
        compression="zstd"
    )


def write_radolan_geoarrow(path):
    file_name = "trees"
    write_geoarrow_content(path, file_name)
