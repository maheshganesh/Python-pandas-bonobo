from typing import List, Dict
import logging
from contextlib import contextmanager
import json
from psycopg2 import pool, DatabaseError

logger = logging.getLogger('etl_python')

class DatabaseConnection:
    """
    Database connection class for connections & execute commands
    """
    def __init__(self, config_file: str) -> None:
        """
        :param config_file: path to json config file
        """
        self.config_file = config_file
        self.configs = self.load_config_file()
        self.connect_pool = pool.SimpleConnectionPool(**self.configs)

    def load_config_file(self) -> Dict:
        """
        Load database configuration from file
        """
        try:
            with open(self.config_file, 'r') as f:
                configs = json.load(f)
                return configs.get('database')
        except FileNotFoundError:
            logger.error('Unable to find configuration file')
            raise FileNotFoundError('configuration file not found')
        except Exception as exp:
            logger.error(f"Error on configuration file: {exp}")
            raise

    @contextmanager
    def get_pool_connection(self):
        connection = self.connect_pool.getconn()
        try:
            yield connection
        except DatabaseError as err:
            logger.error('Error on get connection operation', err)
        except Exception as exp:
            logger.error(f"Error on connecting to database ")
            raise
        finally:
            # Return connection to pool
            self.connect_pool.putconn(connection)
