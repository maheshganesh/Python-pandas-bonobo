"""
Module to perform simple Operation using Bonobo, pandas and then load into database for analysis
"""
from typing import List
import logging
import os
import bonobo
import pandas as pd

from utils import (extract_file, load_data, clean_movie_data,
                   transform_credits, high_rated_director)
from database import DatabaseConnection

console = logging.StreamHandler()
file_handler = logging.FileHandler('Etl-run.log')
logging.basicConfig(level=logging.INFO,
                    handlers=[console, file_handler],
                    format="%(asctime)s :%(levelname)s :%(message)s"
)
logger = logging.getLogger('etl_python')

def extract() -> List:
    """
    Extract data from data location
    """
    logger.info('Start extract process')
    movies_df, credits_df = (None, None)
    file_name_path = f"../data/{os.getenv('file_name', 'movies.csv')}"
    file_type = os.getenv('file_type', 'csv')
    if file_type == 'csv':
        movies_df = extract_file(file_name_path)
    else:
        movies_df = extract_file(file_name_path, file_type)

    credits_df = extract_file('../data/credits.csv')
    logger.info('Files extracted complete')
    return [movies_df, credits_df]

def transform(data: List) -> List:
    """
    Simple transformations to extracted data
    """
    movies_dedup = clean_movie_data(data[0])

    credits_transform = transform_credits(data[1])
    movie_credit_df = pd.merge(
        movies_dedup, credits_transform, on=["id"]
    )
    return [movie_credit_df]

def load_analyze(data: List) -> None:
    """
    Load data to database & analyse result to fetch highest rated director
    """
    config_location = 'db_config.json'
    table_name = 'movies'
    db_conn = DatabaseConnection(config_location)
    logger.info('Loading data to database')
    load_data(df=data[0], table='movies', db_conn=db_conn)
    high_rated_director(table=table_name, db_conn=db_conn)


if __name__ == '__main__':
    # Create graph to run ETL job in sequence
    graph = bonobo.Graph(
        extract,
        transform,
        load_analyze
    )
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(graph)
