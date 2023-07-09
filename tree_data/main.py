import logging
import time

from utils.get_data_from_wfs import read_geojson
from utils.interact_with_database import get_db_engine, add_to_db
from utils.process_data import read_config, transform_new_tree_data

logger = logging.getLogger('root')
FORMAT = "[%(levelname)s %(name)s] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.DEBUG)

start = time.time()

new_trees_paths_list, schema_mapping_dict, schema_calculated_dict, database_dict = read_config()

new_trees = read_geojson(new_trees_paths_list[0])
attribute_list = [
    'id',
    'geometry',
    'strname',
    'haus_nr',
    'artbot',
    'artdtsch',
    'standort_nr',
    'baumhoehe',
    'stammdurch',
    'kronedurch',
    'zuletztakt',
    'gattung',
    'gattungdeutsch2',
    'pflanzjahr',
    'stammumfg',
    'xcoord',
    'ycoord',
    'bezirk'
]
transformed_trees = transform_new_tree_data(
    new_trees=new_trees,
    attribute_list=attribute_list,
    schema_mapping_dict=schema_mapping_dict,
    schema_calculated_dict=schema_calculated_dict
)

logger.info("Adding new trees to database...")
for att in attribute_list:
    if att in transformed_trees:
        print(transformed_trees[att])

db_engine = get_db_engine()
add_to_db(db_engine, transformed_trees, 'trees')

end = time.time() - start
logger.info("It took {} seconds to run the script".format(end))