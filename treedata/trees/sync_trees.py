from sqlalchemy import text, Engine
import logging
import os
from multiprocessing import Pool
from functools import partial
import numpy as np


global_engine: Engine


def create_trees_table():
    with global_engine.connect() as conn:
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS "public"."trees" (
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
                "zuletztakt" timestamp,
                "adopted" text,
                "watered" text,
                "radolan_sum" int4,
                "radolan_days" _int4,
                "gebiet" text,
                "letzte_bewaesserung" text,
                "nachpflanzung_geplant" text,
                "status_patenschaft" text,
                "patenschaftsnummer" text,
                "standzeitraum" text,
                PRIMARY KEY ("id")
            );        
        '''))
        conn.commit()


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


def initializer():
    """ensure the parent proc's database connections are not touched in the new connection pool"""
    global_engine.dispose(close=False)


def process_trees(db_fun, process_callback):
    ranges = create_ranges()
    process_count = os.cpu_count() - 1
    sub_ranges_list = [
        ranges[i: i + process_count] for i in range(0, len(ranges), process_count)
    ]

    for sub_ranges in sub_ranges_list:
        with Pool(processes=process_count) as pool:
            for r in sub_ranges:
                year_range = get_year_range_stmt(sub_ranges, r)

                def run_stmt():
                    return db_fun(year_range)

                process_callback_partial = partial(process_callback, r)
                pool.apply_async(run_stmt(), callback=process_callback_partial)
            pool.close()
            pool.join()


def _delete_removed_trees(original_tree_table, tmp_tree_table, year_range):
    with global_engine.connect() as conn:
        return conn.execute(text(f'''
            DELETE FROM public."{original_tree_table}" WHERE standortnr IN (
                SELECT B."standortnr" FROM public."{original_tree_table}" AS B
                WHERE {year_range}
                AND B."standortnr" NOT IN (
                    SELECT A."standortnr" FROM public."{tmp_tree_table}" AS A
                ) 
                AND B."standortnr" not like 'osm_%'
            )        
        '''))


def delete_callback(curr_range, tree_count):
    logging.info(f"Deleted {tree_count} trees in age range {curr_range}.")


def delete_removed_trees(original_tree_table, tmp_tree_table):
    db_fun = partial(_delete_removed_trees, original_tree_table, tmp_tree_table)
    process_callback = partial(delete_callback)
    process_trees(db_fun, process_callback)


def _insert_added_trees(original_tree_table, tmp_tree_table, year_range):
    with global_engine.connect() as conn:
        # TODO id should be generated by database, but the current Supabase creates it as text NOT NULL only
        return conn.execute(text(f'''
            INSERT INTO public."{original_tree_table}" ("id")
            SELECT B."id" FROM public."{tmp_tree_table}" AS B
            WHERE {year_range} 
            AND B."id" IS NOT NULL AND B."id" NOT IN (
                SELECT A."id" FROM public."{original_tree_table}" AS A
            )        
        '''))


def insert_callback(curr_range, tree_count):
    logging.info(f"Inserted {tree_count} trees in age range {curr_range}.")


def insert_added_trees(original_tree_table, tmp_tree_table):
    db_fun = partial(_insert_added_trees, original_tree_table, tmp_tree_table)
    process_callback = partial(insert_callback)
    process_trees(db_fun, process_callback)


def _update_trees(original_tree_table, tmp_tree_table, year_range):
    sql_update_str = f'''
        WITH subquery AS (
            SELECT B."id", B."lat", B."lng", B."artdtsch", B."artbot", B."gattungdeutsch", B."gattung", 
                   B."standortnr", B."strname", B."pflanzjahr", B."stammumfg", 
                   B."kronedurch", B."baumhoehe", B."bezirk", B."geom", B."aend_dat",
                   B."gebiet", B."letzte_bewaesserung", B."nachpflanzung_geplant",
                   B."status_patenschaft", B."patenschaftsnummer", B."standzeitraum"
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
        "zuletztakt" = B."aend_dat",
        "gebiet" = B."gebiet",
        "letzte_bewaesserung" = B."letzte_bewaesserung",
        "nachpflanzung_geplant" = B."nachpflanzung_geplant",
        "status_patenschaft" = B."status_patenschaft",
        "patenschaftsnummer" = B."patenschaftsnummer",
        "standzeitraum" = B."standzeitraum"
        FROM subquery AS B
        WHERE A."id" = B."id";
    '''
    with global_engine.connect() as conn:
        return conn.execute((text(sql_update_str)))


def update_callback(curr_range, tree_count):
    logging.info(f"Updated {tree_count} trees in age range {curr_range}.")


def update_trees(original_tree_table, tmp_tree_table):
    db_fun = partial(_update_trees, original_tree_table, tmp_tree_table)
    process_callback = partial(update_callback)
    process_trees(db_fun, process_callback)


def sync_trees(engine, original_tree_table, tmp_tree_table):
    global global_engine
    global_engine = engine
    create_trees_table()
    delete_removed_trees(original_tree_table, tmp_tree_table)
    insert_added_trees(original_tree_table, tmp_tree_table)
    update_trees(original_tree_table, tmp_tree_table)
