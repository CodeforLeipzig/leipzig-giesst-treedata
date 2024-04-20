import geopandas
from sqlalchemy import text
import logging
from shapely.wkt import loads, dumps
import pandas
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


def exist_radolan_geometry(engine):
    with engine.connect() as conn:
        count = conn.execute(text('SELECT count(*) FROM public.radolan_geometry')).scalar()
        if count > 0:
            return True
    return False


def update_radolan_geometry(engine, radolan_grid_shape_path):
    df = geopandas.read_file(radolan_grid_shape_path)
    df = df.to_crs("epsg:4326")
    clean = df[(df['MYFLD'].notnull())]
    if len(clean) > 0:
        values = []
        for index, row in clean.iterrows():
            values.append(dumps(row.geometry, rounding_precision=5))
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM public.radolan_geometry"))
            conn.commit()
            query = "INSERT INTO public.radolan_geometry (geometry) VALUES {}"
            sub_lists = []
            for index in range(0, len(values), 90):
                sub_lists.append(values[index:index + 90])
            logger.info(f"{len(sub_lists)} batches of geometries to update")
            for index, sub_list in enumerate(sub_lists):
                try:
                    tuples = ", ".join([f"(ST_GeomFromText('{x}', 4326))" for x in sub_list])
                    conn.execute(text(query.format(tuples)))
                    conn.commit()
                    logger.info(f"Updated {index}. {len(sub_list)} geometries")
                except (Exception, psycopg2.DatabaseError) as error:
                    logger.error(f"Error: {error}")
        with engine.connect() as conn:
            conn.execute(text("UPDATE public.radolan_geometry SET centroid = ST_Centroid(geometry)"))
            conn.commit()
    else:
        logger.warning(f"No non null MYFLD values found in {radolan_grid_shape_path}")
    return len(clean)


def upload_radolan_data(engine, radolan_data):
    radolan_data = radolan_data.rename(columns={'MYFLD': 'value'})
    radolan_data['measured_at'] = pandas.to_datetime(radolan_data['measured_at'])
    radolan_data['geometry'] = loads(dumps(radolan_data['geometry'], rounding_precision=5))
    radolan_data.to_postgis('radolan_temp', engine, if_exists='replace', index=False)
    with engine.connect() as conn:
        result = conn.execute(text('''
            INSERT INTO "public".radolan_data(geom_id, value, measured_at) 
            SELECT radolan_geometry.id, sum(radolan_temp.value), radolan_temp.measured_at 
            FROM radolan_geometry JOIN radolan_temp 
            ON ST_WithIn(radolan_geometry.centroid, radolan_temp.geometry)
            GROUP BY radolan_geometry.id, radolan_temp.measured_at
        '''))
        conn.commit()
        number_of_rows = result.rowcount
        logger.info(f"Updated radolan_data for {number_of_rows} entries")


def purge_data_older_than_time_limit_days(engine, time_limit_days):
    with engine.connect() as conn:
        result = conn.execute(text(f'''
            DELETE FROM radolan_data 
            WHERE measured_at < NOW() - INTERVAL '{time_limit_days} days'
        '''))
        conn.commit()
        number_of_rows = result.rowcount
        logger.info(f"Purged radolan_data older than {time_limit_days} days: {number_of_rows}")


def purge_duplicates(engine):
    with engine.connect() as conn:
        result = conn.execute(text('''
            DELETE FROM radolan_data AS a USING radolan_data AS b 
            WHERE a.id < b.id AND a.geom_id = b.geom_id 
            AND a.measured_at = b.measured_at
        '''))
        conn.commit()
        number_of_rows = result.rowcount
        logger.info(f"Purged duplicate radolan_data: {number_of_rows}")
