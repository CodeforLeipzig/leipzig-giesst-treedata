import math
import csv

from sqlalchemy import text


def get_trees_with_radolan_data_total(engine, time_limit_days):
    with engine.connect() as conn:
        cursor = conn.execute(text(f'''
        SELECT 
          trees.external_tree_id as id, 
          trees.lng, 
          trees.lat, 
          (
            trees.radolan_sum + (
              SELECT COALESCE(SUM (CAST (w.amount AS integer)) * 10, 0) 
              FROM trees_watered w 
              WHERE timestamp >= NOW() - interval '{time_limit_days} day' 
              AND w.tree_id = trees.id
            )
          ) as radolan_sum, 
          CASE 
            WHEN trees.pflanzjahr > 1000 THEN date_part('year', CURRENT_DATE) - trees.pflanzjahr 
            ELSE NULL 
          END AS age 
        FROM trees 
        WHERE ST_CONTAINS(
          ST_SetSRID(
            (SELECT ST_EXTENT(geometry) FROM radolan_geometry), 
            4326
          ), 
          trees.geom
        )
      '''))
    return cursor.fetchall()


def get_trees_with_radolan_data(engine):
    with engine.connect() as conn:
        cursor = conn.execute(text(f'''
      SELECT 
        trees.external_tree_id as id, 
        trees.lng, 
        trees.lat, 
        trees.radolan_sum, 
        CASE 
          WHEN trees.pflanzjahr > 1000 THEN date_part('year', CURRENT_DATE) - trees.pflanzjahr 
          ELSE NULL 
        END AS age 
      FROM trees 
      WHERE ST_CONTAINS(
        ST_SetSRID(
          (SELECT ST_EXTENT(geometry) FROM radolan_geometry), 
          4326
        ), 
        trees.geom
      )
    '''))
    return cursor.fetchall()


def write_trees_csv_total(engine, time_limit_days, path):
    trees = get_trees_with_radolan_data_total(engine, time_limit_days)
    file_name = "trees-total"
    return write_csv_content(trees, path, file_name)


def write_trees_csv(engine, path):
    trees = get_trees_with_radolan_data(engine)
    file_name = "trees"
    return write_csv_content(trees, path, file_name)


def get_tree_csv_row_values(tree):
    id = tree[0]
    lng = tree[1]
    lat = tree[2]
    radolan_sum = tree[3]
    age = tree[4]
    if age is not None:
        age = int(age)
    else:
        # default age 10, so that trees without age are still displayed
        age = 10
    return [id, lng, lat, radolan_sum, age]


def write_csv_content(trees, path, file_name):
    filepath_to_filename = {}
    column_names = ['id', 'lng', 'lat', 'radolan_sum', 'age']
    trees_per_file_limit = math.ceil(len(trees) / 4)
    trees_sublists = []
    if len(trees) > 0:
        for index in range(0, len(trees), trees_per_file_limit):
            trees_sublists.append(trees[index:index + trees_per_file_limit])
    else:
        raise Exception(f"Error: trees is empty for {path}{file_name}.csv")

    file_path = f"{path}{file_name}.csv"
    filepath_to_filename[file_path] = f"{file_name}.csv"
    with open(file_path, mode='w') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(column_names)
        for sublist_index, trees_sublist in enumerate(trees_sublists):
            percentile_file_name = f"{file_name}-p{sublist_index + 1}.csv"
            percentile_file_path = f"{path}{percentile_file_name}"
            filepath_to_filename[percentile_file_path] = percentile_file_name
            with open(percentile_file_path, mode='w') as percentile_csv_file:
                percentile_csv_writer = csv.writer(percentile_csv_file, delimiter=',', quotechar='"',
                                                   quoting=csv.QUOTE_MINIMAL)
                percentile_csv_writer.writerow(column_names)
                for index, tree in enumerate(trees_sublist):
                    row = get_tree_csv_row_values(tree)
                    percentile_csv_writer.writerow(row)
                    csv_writer.writerow(row)
    return filepath_to_filename


def write_radolan_csvs(engine, time_limit_days, path):
    total_files_dict = write_trees_csv_total(engine, time_limit_days, path)
    files_dict = write_trees_csv(engine, path)
    return total_files_dict | files_dict
