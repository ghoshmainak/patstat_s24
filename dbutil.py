"""
Utility function for connecting database

@author: Mainak Ghosh

"""
from sqlalchemy import create_engine, text
import pymysql
from typing import Any, Dict, List, Optional
import pandas as pd


__MYSQL_CONN_STRING__ = 'mysql+pymysql://{user}:{password}@s-harhoffdb1'


def get_connection(user_name: str, password: str) -> Any:
    """Provide your MySQL credentials and schema name to
    get a connection to that schema
    Parameters
    ----------
    schema_name : str
    user_name : str
    password : str
    Returns
    -------
    Connection
    """
    connection_string = __MYSQL_CONN_STRING__.format(user=user_name, password=password)
    conn = create_engine(connection_string).connect()
    return conn


def close_connection(conn: Any):
    """Closing connection to schema
    Parameters
    ----------
    conn : Connection
    Returns
    -------
    None.
    """
    conn.close()


def execute_query(user_name: str, password: str, 
                  query: str, query_param: Dict, 
                  conn: Optional[Any]=None, 
                  keep_conn_alive: Optional[bool]=False) -> pd.DataFrame:
    """
    Given a query and necessary parameters, this function executes the query
    and return the result in a DataFrame
    Parameters
    ----------
    user_name : str
        db username.
    password : str
        db password.
    query : str
        DESCRIPTION.
    query_param : Dict
        DESCRIPTION.
    Returns
    -------
    Pandas DataFrame.
    """
    if conn is None:
        conn = get_connection(user_name, password)
        print('Connection established')
    
    list_df: List[pd.DataFrame] = []
    try:
        for chunk in pd.read_sql(text(query), conn, params=query_param, chunksize=1000):
            list_df.append(chunk)
    finally:
        if not keep_conn_alive:
            close_connection(conn)
            print('connection closed')
    if len(list_df) > 0:
        return pd.concat(list_df, ignore_index=True)
    else:
        return None
