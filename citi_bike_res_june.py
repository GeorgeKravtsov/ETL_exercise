import pandas as pd
from geopy.distance import distance
from geopy import Point
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'depends_on_past': False,
}

def distance_eval(row):
        start = Point(row['start_lat'], row['start_lng'])
        stop = Point(row['end_lat'], row['end_lng'])
        return distance(start, stop).km

def speed_eval(row):
    if row.distance_km != 0 and row.trip_duration != 0:
        return row.distance_km / (float(row.trip_duration.total_seconds()) / 3600)
    else:
        return 0.0    

    
def create_dataframe():
    df = pd.read_csv('/absolute_path/202206-citibike-tripdata.csv', delimiter=',')
    df = df.dropna().drop_duplicates()
    df['started_at'] = pd.to_datetime(df['started_at'], errors='coerce')
    df['ended_at'] = pd.to_datetime(df['ended_at'], errors='coerce')
    df['trip_duration'] = df.apply(lambda row: row.ended_at - row.started_at, axis=1)
    df['distance_km'] = df.apply(lambda row: distance_eval(row), axis=1)
    df['avg_speed_kmh'] = df.apply(lambda row: speed_eval(row), axis=1)
    df.to_csv('/absolute_path/june2022.csv', sep=',', index=False)
    df['trip_duration'] = df['trip_duration'].astype(str)

    def deal_with_postgres(username, password, sql_query, db_name=None):
        database_loc = f"postgresql://{username}:{password}@localhost:5432"
        if db_name:
            database_loc += '/' + db_name
        engine = create_engine(database_loc, isolation_level='AUTOCOMMIT')
        with engine.connect() as conn:
            conn.execute('commit')
            conn.execute(sql_query)

    username = '***'
    password = '***'

    create_table_query = 'CREATE TABLE IF NOT EXISTS june2022 ( \
                ride_id VARCHAR(16) PRIMARY KEY NOT NULL, \
                rideable_type VARCHAR(13), \
                started_at TIMESTAMP, \
                ended_at TIMESTAMP, \
                start_station_name VARCHAR(44), \
                start_station_id VARCHAR(5), \
                end_station_name VARCHAR(44), \
                end_station_id VARCHAR(7), \
                start_lat NUMERIC, \
                start_lng NUMERIC, \
                end_lat NUMERIC, \
                end_lng NUMERIC, \
                member_casual VARCHAR(6), \
                trip_duration INTERVAL, \
                distance_km NUMERIC, \
                avg_speed_kmh NUMERIC \
            );'

    deal_with_postgres(username=username,
        password=password,
        sql_query=create_table_query,
        db_name='citi_bike_research')

    engine = create_engine(f"postgresql://{username}:{password}@localhost:5432/citi_bike_research")
    df.to_sql(name='june2022',
        con=engine,
        index=False,
        if_exists='append'
        )

with DAG(
    dag_id='june2022',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id='create_df',
        python_callable=create_dataframe,
        dag=dag
    )

    t1
