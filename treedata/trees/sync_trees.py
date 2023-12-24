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
                "aend_dat" timestamp,
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
    with engine.connect() as conn:
        # TODO id should be generated by database, but the current Supabase creates it as text NOT NULL only
        result = conn.execute(text(f'''
            INSERT INTO public."{original_tree_table}" ("id")
            SELECT id FROM public."{tmp_tree_table}" AS B
            WHERE B."id" IS NOT NULL AND B."id" NOT IN (
                SELECT "id" FROM public."{original_tree_table}" AS A
            )        
        '''))
        logging.info(f"Inserted {result.rowcount} trees.")
        conn.commit()


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
        "aend_dat" = B."aend_dat"
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
