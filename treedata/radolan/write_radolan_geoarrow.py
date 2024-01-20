import pandas as pd
import geopandas as gpd
import pyarrow as pa
import pyarrow.feather as feather
from lonboard._geoarrow.geopandas_interop import geopandas_to_geoarrow
import pyarrow.parquet as pq
from lonboard.colormap import apply_continuous_cmap
from palettable.colorbrewer.diverging import RdYlGn_11


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

    def get_color(row):
        age = row["age"]
        radolan_sum = row["radolan_sum"]
        if age < 5:
            return 300
        elif age > 15:
            return 300
        else:
            return radolan_sum
    gdf['water_need'] = gdf.apply(get_color, axis=1)

    colors = apply_continuous_cmap(gdf['water_need'], RdYlGn_11)
    table = table.append_column(
        "colors", pa.FixedSizeListArray.from_arrays(colors.flatten("C"), 3)
    )
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
