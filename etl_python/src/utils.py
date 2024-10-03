"""
Module for storing Extract, Transformation & Loading Logic used on Main file
"""
import logging
from ast import literal_eval
import json
from typing import List, Optional
import numpy as np
from pandas import DataFrame
import pandas as pd
import psycopg2.extras as extras

from database import DatabaseConnection

# List of relevant columns
COL_LIST = [
    'id',
    'title',
    'tagline',
    'release_date',
    'genres',
    'production_companies',
    'vote_count',
    'vote_average'
]
logger = logging.getLogger('etl_python')


def extract_file(filepath: str, file_format='csv') -> DataFrame:
    """
       Extract data function
       :output: dataframe, extracted from CSV data
    """
    try:
        if file_format == 'csv':
            df = pd.read_csv(filepath)
        elif file_format == 'json':
            with open(filepath, 'r') as json_file:
                data = json.load(json_file)

            df = pd.json_normalize(data, sep='_')
            df.rename(columns={
                'genres_name': 'genres',
                'production_companies_name': 'production_companies'
            },inplace=True)
            # filter based on columns list
            df = df[COL_LIST]
            logger.info('Extracted data from json file')
        else:
            raise ValueError("Error Unsupported file format")
    except FileNotFoundError as exp:
        logger.error('Error file not found')
        raise FileNotFoundError('Error file not found')
    except Exception as exp:
        logger.error(f'Error on reading file {exp}')
        raise
    logger.info("Datafile fetched successfully")
    return df


def clean_movie_data(df: DataFrame) -> DataFrame:
    """
       Transform movies data
       :return: dataframe, after transformations
    """
    movies_dedup = df.drop_duplicates(subset=["id"], keep='first')
    movies_dedup['release_date'] = pd.to_datetime(movies_dedup['release_date'], format='%Y-%m-%d')
    return movies_dedup[COL_LIST]

def get_director(values: List) -> Optional[str]:
    """
       Get directory information from json
       :return: director name or None
    """
    for item in values:
        if item['job'] == 'Director':
            return item['name']
    return np.nan

def transform_credits(credit_df: DataFrame) -> DataFrame:
    """
       Transform Credits data
       :return: dataframe, after transformations
    """
    credit_df.drop_duplicates(subset=["id"], keep='first', inplace=True)
    # Get length of crew size
    credit_df.crew = credit_df.crew.apply(
        lambda x: literal_eval(x) if isinstance(x, str) else np.nan)
    credit_df['director'] = credit_df.crew.apply(get_director)

    credit_df["crew_size"] = credit_df.crew.apply(lambda x: len(x))
    credit_df.dropna(subset=["crew_size"], inplace=True)
    credit_df.drop(columns=['crew', 'cast'], inplace=True)
    logger.info('Transformed content for credits df')
    return credit_df

def load_data(df: DataFrame, table: str, db_conn: DatabaseConnection) -> None:
    """
    Load data into postgres database
    :param: df: input dataframe
    :param: table: postgres table name
    :param: db_conn: connection to database
    :return: None
    """
    cols_list = ', '.join(list(df.columns))
    tuple_list = [tuple(rec) for rec in df.to_numpy()]
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols_list)

    with db_conn.get_pool_connection() as connection:
        cursor = connection.cursor()
        extras.execute_values(cursor, query, tuple_list)
        connection.commit()
        cursor.close()

def high_rated_director(table: str, db_conn: DatabaseConnection) -> List:
    """
    Get data from postgres database
    :param: table: postgres table name
    :param: db_conn: connection to database
    :return: List of items
    """
    results = []
    with db_conn.get_pool_connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
                f''' select m.id, m.director, m.title, m.vote_average
                    from {table} m 
                    where m.vote_average >= 6 and m.vote_count > 1000
                    order by m.vote_average desc
                    limit 5'''
        )
        results_db = cursor.fetchall()
        cursor.close()
        for res in results_db:
            item_dict = dict()
            item_dict['director'], item_dict['title'], item_dict['rating'] = res[1], res[2], res[3]
            logger.info(f"Director Name: {res[1]}, Movie Name: {res[2]}, Rating: {res[3]}")
            results.append(item_dict)

        return results
