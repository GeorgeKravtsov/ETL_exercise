import pandas as pd
from geopy.distance import distance
from geopy import Point
from sqlalchemy import create_engine


df = pd.read_csv('202201-citibike-tripdata.csv', delimiter=',')

df = df.dropna().drop_duplicates()

df['started_at'] = pd.to_datetime(df['started_at'], errors='coerce')
df['ended_at'] = pd.to_datetime(df['ended_at'], errors='coerce')
df['trip_duration'] = df.apply(lambda row: row.ended_at - row.started_at, axis=1)

def distance_eval(row):
    start = Point(row['start_lat'], row['start_lng'])
    stop = Point(row['end_lat'], row['end_lng'])
    return distance(start, stop).km

def speed_eval(row):
    if row.distance_km != 0 and row.trip_duration != 0:
        return row.distance_km / (float(row.trip_duration.total_seconds()) / 3600)
    else:
        return 0.0

df['distance_km'] = df.apply(lambda row: distance_eval(row), axis=1)
df['avg_speed_kmh'] = df.apply(lambda row: speed_eval(row), axis=1)
df['trip_duration'] = df['trip_duration'].astype(str) # to save timedelta in postgres; postgres converts timedelta to integer

def deal_with_postgres(username, password, sql_query, db_name=None):
    database_loc = f"postgresql://{username}:{password}@localhost:5432"
    if db_name:
        database_loc += '/' + db_name
    engine = create_engine(database_loc, isolation_level='AUTOCOMMIT')
    with engine.connect() as conn:
        conn.execute("commit")
        conn.execute(sql_query)

username = '***'
password = '***'
sql_query = 'CREATE DATABASE citi_bike_research'

deal_with_postgres(username=username, password=password, sql_query=sql_query)

engine = create_engine(f"postgresql://{username}:{password}@localhost:5432/citi_bike_research")
df.to_sql('january2022', engine)
