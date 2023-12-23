import argparse
from datetime import datetime
from utils.wfs_with_args import configure_wfs_args, handle_wfs

dateFormat = "{:0>2d}"
currentDay = dateFormat.format(datetime.now().day)
currentMonth = dateFormat.format(datetime.now().month)
currentYear = str(datetime.now().year)
dateStr = f"{currentYear}-{currentMonth}-{currentDay}"

wfs_url_base_default = 'https://geodienste.leipzig.de/l3/OpenData/wfs?REQUEST=GetCapabilities&SERVICE=WFS'
layer_default = 'OpenData:Baeume'
base_folder = './resources/trees'
xml_file_name_default = f"s_wfs_baumbestand_{dateStr}"
geojson_file_name_default = f"s_wfs_baumbestand_{dateStr}"


def configure_trees_args(parser=argparse.ArgumentParser(description='Process tree data')):
    configure_wfs_args(
        wfs_url_base_default,
        layer_default,
        xml_file_name_default,
        geojson_file_name_default,
        parser=parser,
    )
    parser.set_defaults(which='trees', func=handle_trees)


def handle_trees(args):
    handle_wfs(args, base_folder)
