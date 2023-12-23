import argparse
from .get_data_from_wfs import download_wfs_to_xml
from .get_data_from_wfs import get_wfs_request_url, convert_xml_to_geojson

source_encoding_default = 'iso-8859-1'


def __download_wfs_to_geojson__(wfs_url_base, layer, xml_file_path, geojson_file_path,
                                source_encoding=source_encoding_default, download_wfs=True, convert_to_geojson=True):
    if download_wfs:
        wfs_url_with_params = get_wfs_request_url(wfs_url_base, layer)
        download_wfs_to_xml(wfs_url=wfs_url_with_params, source_encoding=source_encoding, outfile_path=xml_file_path)

    if convert_to_geojson:
        convert_xml_to_geojson(infile_path=xml_file_path, source_encoding=source_encoding,
                               outfile_path=geojson_file_path)


def configure_wfs_args(
        wfs_url_base_default,
        layer_default,
        xml_file_name_default,
        geojson_file_name_default,
        parser=argparse.ArgumentParser(description='Process city shape'),
):
    parser.add_argument('-u', '--wfs-url', dest='wfs_url_base', action='store', help='Provide WFS URL',
                        default=wfs_url_base_default)
    parser.add_argument('-l', '--layer', dest='layer', action='store', help='Provide Layername',
                        default=layer_default)
    parser.add_argument('-e', '--source-encoding', dest='source_encoding', action='store', help='Provide WFS Encoding',
                        default=source_encoding_default)
    parser.add_argument('-x', '--xml-file-name', dest='xml_file_name', action='store',
                        help='Provide file name to store WFS XML file locally',
                        default=xml_file_name_default)
    parser.add_argument('-j', '--geojson-file-name', dest='geojson_file_name', action='store',
                        help='Provide file name to store GeoJSON file locally',
                        default=geojson_file_name_default)
    parser.add_argument('--skip-download-wfs-xml', dest='skip_download_wfs', action='store_true',
                        help='use existing XML instead downloading anew', default=False)
    parser.add_argument('--skip-convert-to-geojson', dest='skip_convert_to_geojson', action='store_true',
                        help='skip step of converting WFS XML to GeoJSON file', default=False)


def handle_wfs(args, base_folder):
    __download_wfs_to_geojson__(wfs_url_base=args.wfs_url_base,
                                layer=args.layer,
                                source_encoding=args.source_encoding,
                                xml_file_path=f"{base_folder}/{args.xml_file_name}",
                                geojson_file_path=f"{base_folder}/{args.geojson_file_name}",
                                download_wfs=not args.skip_download_wfs,
                                convert_to_geojson=not args.skip_convert_to_geojson)
