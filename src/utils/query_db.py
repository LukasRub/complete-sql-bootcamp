from typing import Generator, Optional
from contextlib import contextmanager

import psycopg2 as pg2
import pandas as pd


class Connection:

    def __init__(self, connection) -> None:
        self.connection = connection

    def fetch_all(self, query: str) -> Optional[tuple[pd.DataFrame, int]]:
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query)
            except pg2.Error as e:
                print(e)
            else:
                columns = [column.name for column in cursor.description]
                results = cursor.fetchall()
                row_count = cursor.rowcount

                return (pd.DataFrame(results, columns=columns), row_count)
            




@contextmanager
def postgres_connect(**kwargs) -> Generator:
    connection = pg2.connect(**kwargs)
    try:
        yield Connection(connection)
    except pg2.Error as e:
        print(e)
    finally:
        connection.close()