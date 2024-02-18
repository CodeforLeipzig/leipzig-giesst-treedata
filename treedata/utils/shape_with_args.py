import argparse
from .get_data_from_shp import download_shp as download_shapefile
from .get_data_from_shp import get_shp_request_url, convert_shp_to_geojson

source_encoding_default = 'cp1252'


def __download_shp_to_geojson__(shp_url_base, shp_file_path, geojson_file_path,
                                source_encoding=source_encoding_default, download_shp=True, convert_to_geojson=True):
    if download_shp:
        shp_url_with_params = get_shp_request_url(shp_url_base)
        download_shapefile(shp_url=shp_url_with_params, outfile_path=shp_file_path)

    if convert_to_geojson:
        convert_shp_to_geojson(infile_path=shp_file_path, source_encoding=source_encoding,
                               outfile_path=geojson_file_path)


def configure_shp_args(
        shp_url_base_default,
        shp_file_name_default,
        geojson_file_name_default,
        parser=argparse.ArgumentParser(description='Process shape'),
):
    parser.add_argument('-u', '--shp-url', dest='shp_url_base', action='store', help='Provide Shapfile URL',
                        default=shp_url_base_default)
    parser.add_argument('-e', '--source-encoding', dest='source_encoding', action='store', help='Provide Shapefile Encoding',
                        default=source_encoding_default)
    parser.add_argument('-s', '--shp-file-name', dest='shp_file_name', action='store',
                        help='Provide file name to store Shapefile zip file locally',
                        default=shp_file_name_default)
    parser.add_argument('-j', '--geojson-file-name', dest='geojson_file_name', action='store',
                        help='Provide file name to store GeoJSON file locally',
                        default=geojson_file_name_default)
    parser.add_argument('--skip-download-shp', dest='skip_download_shp', action='store_true',
                        help='use existing Shapefile instead downloading anew', default=False)
    parser.add_argument('--skip-convert-to-geojson', dest='skip_convert_to_geojson', action='store_true',
                        help='skip step of converting Shapefile to GeoJSON file', default=False)


def handle_shp(args, base_folder):
    __download_shp_to_geojson__(shp_url_base=args.shp_url_base, source_encoding=args.source_encoding,
                                shp_file_path=f"{base_folder}/{args.shp_file_name}",
                                geojson_file_path=f"{base_folder}/{args.geojson_file_name}",
                                download_shp=not args.skip_download_shp,
                                convert_to_geojson=not args.skip_convert_to_geojson)
