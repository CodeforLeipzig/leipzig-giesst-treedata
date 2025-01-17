from sqlalchemy import text
import logging


def create_trees_table(engine):
    with engine.connect() as conn:
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
                PRIMARY KEY ("id")
            );        
        '''))
        conn.commit()


def delete_removed_trees(engine, original_tree_table, tmp_tree_table):
    with engine.connect() as conn:
        result = conn.execute(text(f'''
            DELETE FROM public."{original_tree_table}" WHERE standortnr IN (
                SELECT A."standortnr" FROM public."{original_tree_table}" AS A
                WHERE A."standortnr" NOT IN (
                    SELECT B."standortnr" FROM public."{tmp_tree_table}" AS B
                ) 
                AND A."standortnr" not like 'osm_%'
            )        
        '''))
        logging.info(f"Deleted {result.rowcount} trees.")
        conn.commit()


def insert_added_trees(engine, original_tree_table, tmp_tree_table):
    start_year = 1920
    end_year = 2025
    step = 5
    ranges = [(year, year + step) for year in range(start_year, end_year, step)]
    ranges.append((1500, 1920))
    ranges.append((-1, -1))
    tree_count = 0
    for r in ranges:
        with engine.connect() as conn:
            # TODO id should be generated by database, but the current Supabase creates it as text NOT NULL only
            if ranges[0] == -1:
                year_range = "B.pflanzjahr::int IS NULL"
            else:
                year_range = f"B.pflanzjahr::int > {r[0]} AND B.pflanzjahr::int <= {r[1]}"
            result = conn.execute(text(f'''
                INSERT INTO public."{original_tree_table}" ("id")
                SELECT id FROM public."{tmp_tree_table}" AS B
                WHERE {year_range} 
                AND B."id" IS NOT NULL AND B."id" NOT IN (
                    SELECT "id" FROM public."{original_tree_table}" AS A
                )        
            '''))
            tree_count += result.rowcount
            conn.commit()
    logging.info(f"Inserted {tree_count} trees.")


def updated_trees(engine, original_tree_table, tmp_tree_table):
    sql_update_str = f'''
        WITH subquery AS (
            SELECT B."id", B."lat", B."lng", B."artdtsch", B."artbot", B."gattungdeutsch", B."gattung", 
                   B."standortnr", B."strname", B."pflanzjahr", B."stammumfg", 
                   B."kronedurch", B."baumhoehe", B."bezirk", B."geom", B."aend_dat" 
            FROM public."{tmp_tree_table}" AS B
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
        "zuletztakt" = B."aend_dat"
        FROM subquery AS B
        WHERE A."id" = B."id";
    '''
    with engine.connect() as conn:
        result = conn.execute(text(sql_update_str))
        logging.info(f'Updated {result.rowcount} trees')
        conn.commit()


def sync_trees(engine, original_tree_table, tmp_tree_table):
    create_trees_table(engine)
    delete_removed_trees(engine, original_tree_table, tmp_tree_table)
    insert_added_trees(engine, original_tree_table, tmp_tree_table)
    updated_trees(engine, original_tree_table, tmp_tree_table)
