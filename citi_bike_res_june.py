import pandas as pd
from geopy.distance import distance
from geopy import Point
from citi_bike_research import distance_eval, speed_eval

df = pd.read_csv('202206-citibike-tripdata.csv', delimiter=',')

df = df.dropna().drop_duplicates()

df['started_at'] = pd.to_datetime(df['started_at'], errors='coerce')
df['ended_at'] = pd.to_datetime(df['ended_at'], errors='coerce')
df['trip_duration'] = df.apply(lambda row: row.ended_at - row.started_at, axis=1)

df['distance_km'] = df.apply(lambda row: distance_eval(row), axis=1)
df['avg_speed_kmh'] = df.apply(lambda row: speed_eval(row), axis=1)
df.to_csv('june2022.csv', sep=',', index=False)
df['trip_duration'] = df['trip_duration'].astype(str)