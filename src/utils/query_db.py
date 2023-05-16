from contextlib import closing, contextmanager
from functools import partial, wraps
from typing import Any, Callable, Generator, Optional
import sqlite3

from IPython.display import display
from dotenv import dotenv_values
import pandas as pd
import psycopg2 as pg2


class Connection:

    def __init__(self, connection, base_exception: Exception, 
                 get_column_names_func: Callable[[Any], list[str]]) -> None:
        self.connection = connection
        self.error = base_exception
        self.get_column_names = get_column_names_func

    def fetch_all(self, query: str, 
                  display_results: bool = True) -> Optional[pd.DataFrame]:
        with closing(self.connection.cursor()) as cursor:
            try:
                cursor.execute(query)
            except self.error as e:
                print(e)
            else:
                columns = self.get_column_names(cursor)
                results = pd.DataFrame(cursor.fetchall(), columns=columns)
                if display_results:
                    display(results)
            finally:
                return results



def get_sqlite3_columns(cursor: sqlite3.Cursor) -> list[str]:
    """
    Retrieves column names of the executed query in a SQLite3 database.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        SQLite3 cursor object of the active database connection

    Returns
    -------
    list of str
        List of processed column names as returned by the 
        SQLite.Cursor.decription attribute.

    Notes
    -----
    To remain compatible with the Python DB API, sqlite3.Cursor.description 
    returns a 7-tuple for each column where the last six items of each tuple 
    are `None` [1]_.

    References
    ----------
    .. [1] Python Software Foundation. (n.d.). sqlite3 — DB-API 2.0 interface for SQLite databases.
           Retrieved from https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor.description
    """
    columns = [column[0] for column in cursor.description]
    return columns


@contextmanager
def db_connect(database: str, *args, **kwargs) -> Generator:
    """
    """
    match database.lower():
        case "sqlite3":
            connection_func = sqlite3.connect
            base_exception = sqlite3.Error
            get_columns_func = get_sqlite3_columns
        case "postgresql":
            connection_func = pg2.connect
            base_exception = pg2.Error
            get_columns_func = lambda cursor: [column.name for column in 
                                               cursor.description]
        case _:
            message = "Only SQLite3 and Postgres connections are implemented."
            raise NotImplementedError(message)
  
    with closing(connection_func(*args, **kwargs)) as conn:
        try:
            yield Connection(conn, base_exception, get_columns_func)
        except base_exception as e:
            print(e)


# SQLite decorated context manager - can use this instead of: 
# `db_connect("sqlite3", *args, **kwargs)`
sqlite3_connect = partial(db_connect, "sqlite3")

# PostreSQL decorated context manager - can use this instead of:
# `db_connect("postgresql", *args, **kwargs)`
relevant_env_vars = ["user", "password", "host", "port"]
conn_options = {key.lower():value for key, value 
                in dotenv_values().items()
                if key.lower() in relevant_env_vars}
postgres_connect = partial(db_connect, "postgresql", **conn_options)
