from cgitb import reset
from urllib import response
import requests
import json
import pandas as pd
from pandas import json_normalize
from sqlalchemy import create_engine
import time
import datetime

def get_URL(query:str, page_num:str, date:str, API_KEY:str) -> str:
    URL = f'https://api.nytimes.com/svc/search/v2/articlesearch.json?q={query}'
    URL += f'&page={page_num}&begin_date={date}&end_date={date}'
    URL += f'&api-key={API_KEY}'
    return URL

df = pd.DataFrame()

current_date = datetime.date(2020, 8,29)
page_num = 1


while True:
    URL = get_URL(query='COVID-19', page_num=str(page_num), date=current_date, API_KEY=API_KEY)
    
    response = requests.get(URL)

    data = response.json()

    df_request = json_normalize(data['response'], record_path=['docs'])

    if df_request.empty:
        break

    df = pd.concat([df, df_request])

    time.sleep(6)

    page_num += 1

if len(df['_id'].unique()) < len(df):
    print('There are duplicates in the data')
    df = df.drop_duplicates('_id', keep='first')

if df['headline.main'].isnull().any():
    print('There are missing values in this dataset')
    df = df[df['headline.main'].isnull()==False]

df = df[['headline.main', 'pub_date', 'byline.original', 'web_url']]

df.columns = ['headline', 'date', 'author', 'url']
df.date = pd.to_datetime(df['date'], errors='coerce')

# print(df.info())
# print(df.columns)

new_df = df.reindex(columns=['url', 'headline','date','author'])
# print(new_df)



database_loc = f"postgresql://{username}:{password}@localhost:5432/{database}"
engine = create_engine(database_loc)

df.to_sql(name='articles',
        con=engine,
        index=False,
        if_exists='append')