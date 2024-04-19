import logging
import os
from multiprocessing import Pool
from functools import partial
import psycopg2

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def execute_statement(connection_dict, sql, has_result=True):
    result = 0
    pg_server, pg_port, pg_username, pg_password, pg_database = connection_dict
    try:
        conn = psycopg2.connect(database=pg_database, user=pg_username,
                                password=pg_password, host=pg_server,
                                port=pg_port)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            if has_result:
                result = cursor.rowcount
        except (Exception, psycopg2.Error) as error:
            print("Error while executing statement", error)
        finally:
            conn.commit()
            conn.close()
    except Exception as error:
        print(f"Connection not established to the database {error}")
    return result


def create_trees_table(connection_dict):
    sql = '''CREATE TABLE IF NOT EXISTS "public"."trees" (
                "id" text NOT NULL,
                "lat" text,
                "lng" text,
                "artdtsch" text,
                "artbot" text,
                "gattungdeutsch" text,
                "gattung" text,
                "standortnr" text,
                "strname" text,
                "pflanzjahr" text,
                "stammdurch" text,
                "kronedurch" text,
                "baumhoehe" text,
                "bezirk" text,
                "geom" geometry,
                "aend_dat" timestamp,
                "adopted" text,
                "watered" text,
                "radolan_sum" int4,
                "radolan_days" _int4,
                "gebiet" text,
                "letzte_bewaesserung" text,
                "nachpflanzung_geplant" text,
                "status_patenbaum" text,
                "patenschaftsnummer" text,
                "standzeitraum" text,
                PRIMARY KEY ("id")
            )    
        '''
    return execute_statement(connection_dict, sql, has_result=False)


def create_ranges():
    start_year = 1920
    end_year = 2025
    step = 5
    ranges = [(year, year + step) for year in range(start_year, end_year, step)]
    ranges.append((1500, 1920))
    ranges.append((-1, -1))
    return ranges


def get_year_range_stmt(ranges, r):
    if ranges[0] == -1:
        return "B.pflanzjahr::int IS NULL"
    else:
        return f"B.pflanzjahr::int > {r[0]} AND B.pflanzjahr::int <= {r[1]}"


def process_trees(db_fun, process_callback):
    ranges = create_ranges()
    year_ranges = [get_year_range_stmt(ranges, r) for r in ranges]
    process_count = os.cpu_count() - 1
    with Pool(processes=process_count) as pool:
        results = pool.map(db_fun, year_ranges)
        pool.close()
        pool.join()
        tree_count = sum(result for result in results)
        process_callback(tree_count)


def _delete_removed_trees(connection_dict, original_tree_table, tmp_tree_table, year_range):
    sql = f'''
        DELETE FROM public."{original_tree_table}" WHERE standortnr IN (
            SELECT B."standortnr" FROM public."{original_tree_table}" AS B
            LEFT JOIN public."{tmp_tree_table}" A ON B."standortnr"=A."standortnr"
            WHERE A."standortnr" IS NULL
            AND B."standortnr" IS NOT NULL
            AND {year_range}
            AND B."standortnr" not like 'osm_%'
        )   
    '''
    return execute_statement(connection_dict, sql)


def delete_callback(tree_count):
    logger.info(f"Deleted {tree_count} trees.")


def delete_removed_trees(connection_dict, original_tree_table, tmp_tree_table):
    db_fun = partial(_delete_removed_trees, connection_dict, original_tree_table, tmp_tree_table)
    process_callback = partial(delete_callback)
    process_trees(db_fun, process_callback)


def _insert_added_trees(connection_dict, original_tree_table, tmp_tree_table, year_range):
    sql = f'''
                INSERT INTO public."{original_tree_table}" ("id")
                SELECT B."id" FROM public."{tmp_tree_table}" AS B
                LEFT JOIN public."{original_tree_table}" A ON B.id=A.id
                WHERE A.id IS NULL
                AND B.id IS NOT NULL
                AND {year_range}
            '''
    return execute_statement(connection_dict, sql)


def insert_callback(tree_count):
    logger.info(f"Inserted {tree_count} trees.")


def insert_added_trees(connection_dict, original_tree_table, tmp_tree_table):
    db_fun = partial(_insert_added_trees, connection_dict, original_tree_table, tmp_tree_table)
    process_callback = partial(insert_callback)
    process_trees(db_fun, process_callback)


def _update_trees(connection_dict, original_tree_table, tmp_tree_table, year_range):
    sql = f'''
        WITH subquery AS (
            SELECT B."id", B."lat", B."lng", B."artdtsch", B."artbot", B."gattungdeutsch", B."gattung", 
                   B."standortnr", B."strname", B."pflanzjahr", B."stammumfg", 
                   B."kronedurch", B."baumhoehe", B."bezirk", B."geom", B."aend_dat",
                   B."gebiet", B."letzte_bewaesserung", B."nachpflanzung_geplant",
                   B."status_patenbaum", B."patenschaftsnummer", B."standzeitraum"
            FROM public."{tmp_tree_table}" AS B
            WHERE {year_range}
        )
        UPDATE public."{original_tree_table}" AS A
        SET 
        "lat" = B."lat", 
        "lng" = B."lng", 
        "artdtsch" = B."artdtsch", 
        "artbot" = B."artbot", 
        "gattungdeutsch" = B."gattungdeutsch", 
        "gattung" = B."gattung", 
        "standortnr" = B."standortnr", 
        "strname" = B."strname", 
        "pflanzjahr" = B."pflanzjahr"::int, 
        "stammumfg" = B."stammumfg",            
        "kronedurch" = B."kronedurch",
        "baumhoehe" = B."baumhoehe", 
        "bezirk" = B."bezirk",
        "geom" = B."geom",
        "aend_dat" = B."aend_dat",
        "gebiet" = B."gebiet",
        "letzte_bewaesserung" = B."letzte_bewaesserung",
        "nachpflanzung_geplant" = B."nachpflanzung_geplant",
        "status_patenbaum" = B."status_patenbaum",
        "patenschaftsnummer" = B."patenschaftsnummer",
        "standzeitraum" = B."standzeitraum"
        FROM subquery AS B
        WHERE A."id" = B."id"
    '''
    return execute_statement(connection_dict, sql)


def update_callback(tree_count):
    logger.info(f"Updated {tree_count} trees.")


def update_trees(connection_dict, original_tree_table, tmp_tree_table):
    db_fun = partial(_update_trees, connection_dict, original_tree_table, tmp_tree_table)
    process_callback = partial(update_callback)
    process_trees(db_fun, process_callback)


def _delete_blacklisted_trees(connection_dict, original_tree_table, year_range):
    sql = f'''
        DELETE FROM public."{original_tree_table}"
        WHERE gattung like 'Pflanzstelle'
    '''
    return execute_statement(connection_dict, sql)


def remove_blacklisted_callback(tree_count):
    logger.info(f"Deleted {tree_count} Pflanzstellen.")


def remove_blacklisted(connection_dict, original_tree_table):
    db_fun = partial(_delete_blacklisted_trees, connection_dict, original_tree_table)
    process_callback = partial(remove_blacklisted_callback)
    process_trees(db_fun, process_callback)


def sync_trees(connection_dict, original_tree_table, tmp_tree_table):
    create_trees_table(connection_dict)
    delete_removed_trees(connection_dict, original_tree_table, tmp_tree_table)
    insert_added_trees(connection_dict, original_tree_table, tmp_tree_table)
    update_trees(connection_dict, original_tree_table, tmp_tree_table)
    remove_blacklisted(connection_dict, original_tree_table)
