from typing import Generator, Optional
from functools import partial
from contextlib import contextmanager

import pandas as pd
import psycopg2 as pg2
from dotenv import dotenv_values
from IPython.display import display


class Connection:

    def __init__(self, connection) -> None:
        self.connection = connection

    def fetch_all(self, query: str) -> Optional[pd.DataFrame]:
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query)
            except pg2.Error as e:
                print(e)
            else:
                columns = [column.name for column in cursor.description]
                results = cursor.fetchall()

                return display(pd.DataFrame(results, columns=columns))


@contextmanager
def postgres_connect(**kwargs) -> Generator:
    connection = pg2.connect(**kwargs)
    try:
        yield Connection(connection)
    except pg2.Error as e:
        print(e)
    finally:
        connection.close()


conn_options = {key.lower():value for key, value in dotenv_values().items()}
db_connection = partial(postgres_connect, **conn_options)