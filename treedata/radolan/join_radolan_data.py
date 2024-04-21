import logging
import os
import pandas
import geopandas
from datetime import datetime

ROOT_DIR = os.path.abspath(os.curdir)
path = f"{ROOT_DIR}/resources/radolan"


def join_radolan_data():
    filelist = []
    for (dirpath, dirnames, filenames) in os.walk(path):
        for filename in filenames:
            if ("RW_" in filename) and (".shp" in filename):
                filelist.append(path + "/" + filename)

    gdf = None
    if len(filelist) == 0:
        raise Exception("No radolan shp files found")
    for counter, file in enumerate(filelist):
        file_split = file.split("/")
        file_name = file_split[len(file_split) - 1].split('.')[0]
        try:
            date_time_obj = datetime.strptime(file_name, 'RW_%Y%m%d-%H%M')
        except Exception as e:
            raise Exception(f"Exception {e} at {file} in {file_name}")

        df = geopandas.read_file(file)
        df = df.to_crs("epsg:3857")

        # if there was no rain on that timestamp, there will be no data to insert
        if df['geometry'].count() > 0:
            clean = df[(df['MYFLD'] > 0) & (df['MYFLD'].notnull())]
            if len(clean) > 0:
                logging.info("🌧 Found some rain for file " + file)
                df['measured_at'] = date_time_obj.strftime('%Y-%m-%d')
                if gdf is None:
                    gdf = df
                else:
                    gdf = pandas.concat([gdf, df], ignore_index=True)
            else:
                logging.debug("clean is 0 for file " + file)
        else:
            logging.info("no geometry found in file " + file)
    return gdf
