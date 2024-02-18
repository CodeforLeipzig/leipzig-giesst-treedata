import os
import ssl
import logging
import geopandas as gpd
from requests import Request, get

from requests import Request, get

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
        getattr(ssl, '_create_unverified_context', None)):
    ssl._create_default_https_context = ssl._create_unverified_context


def get_shp_request_url(shp_url):
    return Request('GET', shp_url).prepare().url


def download_shp(shp_url, outfile_path, chunk_size=128):
    logger.info(f"Request Shapefile from {shp_url}")
    r = get(shp_url, stream=True)
    with open(f"{outfile_path}.zip", 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


def convert_shp_to_geojson(infile_path, source_encoding, outfile_path):
    logger.info(f'Load Shapefile {infile_path}.zip')
    data = gpd.read_file(f"{infile_path}.zip", encoding=source_encoding, crs_wkt='2100')
    store_as_geojson(data, outfile_path)


def store_as_geojson(data, outfile_name):
    geojson_str = data.to_json(to_wgs84=True)
    with open(f"{outfile_name}.geojson", 'w', encoding='utf-8') as fd:
        fd.write(geojson_str)
    logger.info(f"WFS was written to file {outfile_name}.geojson")


def read_geojson(infile_name):
    return gpd.read_file(infile_name, encoding='utf-8') # (..., rows=50) for testing
