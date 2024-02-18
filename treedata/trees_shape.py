import argparse
from datetime import datetime
from utils.shape_with_args import configure_shp_args, handle_shp

dateFormat = "{:0>2d}"
currentDay = dateFormat.format(datetime.now().day)
currentMonth = dateFormat.format(datetime.now().month)
currentYear = str(datetime.now().year)
dateStr = f"{currentYear}-{currentMonth}-{currentDay}"

shp_url_base_default = ('https://opendata.leipzig.de/dataset/8024d039-9b75-4154-a4ad-05968094f4eb/resource/'
                        'bed9cbfb-2b3e-416c-a071-11422861fd5f/download/strassenbaumkatasterleipzig12032020.zip')
base_folder = './resources/trees'
shp_file_name_default = f"shp_baumbestand_{dateStr}"
geojson_file_name_default = f"shp_baumbestand_{dateStr}"


def configure_trees_args(parser=argparse.ArgumentParser(description='Process tree data')):
    configure_shp_args(
        shp_url_base_default,
        shp_file_name_default,
        geojson_file_name_default,
        parser=parser,
    )
    parser.set_defaults(which='trees', func=handle_trees)


def handle_trees(args):
    handle_shp(args, base_folder)
