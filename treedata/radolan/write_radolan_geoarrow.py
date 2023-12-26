import pandas as pd
import geopandas as gpd
import pyarrow as pa
import pyarrow.feather as feather
from lonboard._geoarrow.geopandas_interop import geopandas_to_geoarrow
import pyarrow.parquet as pq
from lonboard.colormap import apply_continuous_cmap
from palettable.colorbrewer.diverging import RdYlGn_11


def write_geoarrow_content(path, file_name):
    df = pd.read_csv(f"{path}{file_name}.csv")
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.lng, df.lat)
    )
    gdf = gdf.drop(columns=["lng", "lat", "radolan_sum"])
    table = geopandas_to_geoarrow(gdf, preserve_index=False)

    age = gdf["age"]

    colors = apply_continuous_cmap(age, RdYlGn_11)
    table = table.append_column(
        "colors", pa.FixedSizeListArray.from_arrays(colors.flatten("C"), 3)
    )
    table = table.drop_columns(columns=["age"])

    file_path = f"{path}{file_name}.feather"
    feather.write_feather(
        table, file_path, compression="uncompressed"
    )
    pq.write_table(table, f"{path}{file_name}.parquet")


def write_radolan_geoarrow(path):
    file_name = "trees"
    write_geoarrow_content(path, file_name)
