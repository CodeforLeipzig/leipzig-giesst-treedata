import argparse
import logging
import os
import time
from trees_wfs import configure_trees_args
from trees_shape import configure_trees_args as configure_trees_shape_args
from trees_process import configure_trees_process_args
from weather import configure_weather_args
from dotenv import load_dotenv
from multiprocessing import freeze_support


ROOT_DIR = os.path.abspath(os.curdir)
FORMAT = "[%(levelname)s %(name)s] %(message)s"


def main():
    freeze_support()

    logger = logging.getLogger('root')
    logging.basicConfig(format=FORMAT)
    logger.setLevel(logging.DEBUG)
    logging.getLogger("fiona.ogrext").setLevel(logging.WARNING)

    start = time.time()
    load_dotenv(f'{ROOT_DIR}/resources/.env')

    parser = argparse.ArgumentParser(description='Processing city shape, tree data and weather data')
    subparsers = parser.add_subparsers(help='actions', dest='action')

    trees_parser = subparsers.add_parser('trees', help="Download and process tree data")
    configure_trees_args(trees_parser)

    trees_shape_parser = subparsers.add_parser('trees-shp', help="Download and process tree data from shapefile")
    configure_trees_shape_args(trees_shape_parser)

    trees_process_parser = subparsers.add_parser('trees_process', help="Transform and upload tree data")
    configure_trees_process_args(trees_process_parser)

    weather_parser = subparsers.add_parser('weather', help="Download and process DWD radolan data")
    configure_weather_args(weather_parser)

    res = parser.parse_args()
    res.func(res)

    end = time.time() - start
    logger.info("It took {} seconds to run the script".format(end))


if __name__ == "__main__":
    main()
