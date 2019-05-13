"""
Generic helpful functions for using SQL within Python
"""

import psycopg2
import numpy as np
import pandas as pd
import sqlalchemy
import yaml


"""
Reads in a .yaml configuration file
    Given: string of .yaml file name
    Returns: dictionary of configuration
"""
def read_yaml(f):
    f = open(f, 'r')
    return yaml.load(f)


"""
Creates an engine for sql queries
    Given: string of .yaml file name
    Returns: engine
"""
def create_engine(filename):
    creds = read_yaml(filename)
    engine_path = 'postgresql://{}:{}@{}/{}'
    # TODO: give error if credentials are wrong
    return sqlalchemy.create_engine(engine_path.format(creds['db']['username'],
        creds['db']['password'], creds['db']['host'], 
        creds['db']['database']))


"""
Performs a query
    Given: a string of the desired query, engine
    Returns: result of query
"""
def query(qry, engine):
    print("Querying...")
    print("\n" + qry + "\n")
    df = pd.read_sql_query(qry, engine)
    print("Query complete.")
    return df


"""
Performs a query with no return values
    Given: a string of desired query, engine
    Returns: nothing
"""
def query_no_return(qry, engine):
    conn = engine.connect()
    trans = conn.begin()
    print("Querying........")
    print("\n" + qry + "\n")
    conn.execute(qry, engine)
    trans.commit()
    conn.close()
    print("Query complete.")


"""
Performs a query specifically to append to an existing table
    Given: pandas data frame with the same columns as existing table,
        engine
    Does: appends to existing table
    Returns: nothing
"""
def append_table(df, schema, table, engine):
    df.to_sql(table, engine, schema = schema, if_exists = 'append')
