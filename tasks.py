import pandas as pd
import numpy as np
import os
import datetime
import pandasql


def read_impression_file(file_name):
    impression_df = pd.read_json(file_name, encoding='utf-8')
    impression_df[['advertiser_id', 'app_id']] = impression_df[['advertiser_id', 'app_id']].fillna(-1)
    impression_df[['advertiser_id', 'app_id']] = impression_df[['advertiser_id', 'app_id']].astype(np.int32)
    return impression_df


def process_impression_file(file_name):
    directory = 'data/raw_impression'
    df = read_impression_file(file_name)
    df.to_parquet(directory, partition_cols=['app_id', 'advertiser_id'])


def load_raw_impressions():
    return pd.read_parquet('data/raw_impression')


def process_click_file(file_name):
    directory = 'data/click'
    ingestion_dt = str(datetime.datetime.now())[:10]
    df = pd.read_json(file_name, encoding='utf-8')
    df['dt'] = ingestion_dt
    df.to_parquet(directory, partition_cols=['dt'])


def load_clicks():
    return pd.read_parquet('data/click')


def process_deduped_impressions():
    directory = 'data/deduped_impression'
    df = load_raw_impressions()
    df.drop_duplicates(subset=['id'], inplace=True)
    df.to_parquet(directory, partition_cols=['app_id', 'advertiser_id'])


def load_deduped_impressions():
    return pd.read_parquet('data/deduped_impression')


def attribute_impressions(impressions, clicks):
    with open("resources/attribute_impressions.ql", "r") as input:
        query = input.read()

    df = pandasql.sqldf(query, locals())
    df.to_parquet('data/impressions', partition_cols=['app_id', 'advertiser_id'])


def load_impressions():
    #return pd.read_csv('impression.csv')
    return pd.read_parquet('data/impressions')