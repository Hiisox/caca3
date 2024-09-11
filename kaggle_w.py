import pandas as pd
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi
import os
import psycopg2
from sqlalchemy import create_engine, text

def script():
    prep_file()
    create_final_csv()
    create_table()
    fill_table()


def fill_table():
    db_url = f"postgresql+psycopg2://postgres:password@localhost:5432/Btc-db"
    engine = create_engine(db_url)
    my_csv  = pd.read_csv("btc_stats.csv", index_col=None)
    my_csv.to_sql("performance", engine, if_exists='replace', index=False)


def create_table():
    with psycopg2.connect("dbname=Btc-db user=postgres password=password host=localhost port=5432") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                        CREATE TABLE IF NOT EXISTS performance(
                        month VARCHAR(200) PRIMARY KEY ,
                        positive INT,
                        negative INT,
                        percentage_positive FLOAT,
                        perf_average FLOAT)""")
        conn.commit()


def create_final_csv():
    btc_monthly = monthly_dataframe()
    final_df = pd.DataFrame(columns=['month', 'positive', 'negative', 'percentage_positive', 'perf_average'])
    final_df.set_index('month', inplace=True)
    for x in range(12):
        final_df = pd.concat([final_df, return_month(btc_monthly[btc_monthly.index.str.endswith(str(x + 1).zfill(2))], x)])
    final_df = final_df.sort_values(by='positive', ascending=False)
    final_df.to_csv("btc_stats.csv")
    os.remove("BTC-USD.csv")
    return

def return_month(monthly_data: object, x: int) -> object:
    month_list = ["January", "February", "March", "April", "May", "June", "July", "August",
     "September", "October", "November", "December"]
    positive = len(monthly_data.loc[monthly_data.Performance >= 0])
    negative = len(monthly_data.loc[monthly_data.Performance < 0])
    mean = monthly_data.Performance.mean()
    per_positive = (positive / (positive + negative)) * 100
    returned_object = pd.DataFrame([[month_list[x], positive, negative, per_positive, mean]], columns=['month', 'positive', 'negative', 'percentage_positive', 'perf_average'])
    returned_object.set_index('month', inplace=True)
    return returned_object

def monthly_dataframe() -> object: 
    btc_dataset = pd.read_csv("BTC-USD.csv")
    btc_dataset['Date'] = pd.to_datetime(btc_dataset['Date'])
    btc_dataset.set_index('Date', inplace=True)
    btc_monthly = pd.DataFrame()
    btc_monthly['Open'] = btc_dataset['Open'].resample('ME').first()
    btc_monthly['Close'] = btc_dataset['Close'].resample('ME').last()
    btc_monthly['High'] = btc_dataset['High'].resample('ME').max()
    btc_monthly['Low'] = btc_dataset['Low'].resample('ME').min()
    btc_monthly['Volume'] = btc_dataset['Volume'].resample('ME').sum()
    btc_monthly['Performance'] = ((btc_monthly['Close'] - btc_monthly['Open']) / btc_monthly['Open']) * 100
    btc_monthly.index = btc_monthly.index.strftime("%Y-%m")
    return btc_monthly


def prep_file():
    api = KaggleApi()
    api.dataset_download_files("meetnagadia/bitcoin-stock-data-sept-17-2014-august-24-2021")
    with zipfile.ZipFile("bitcoin-stock-data-sept-17-2014-august-24-2021.zip") as zip_file:
        zip_file.extractall(".")
    os.remove('./bitcoin-stock-data-sept-17-2014-august-24-2021.zip')

script()