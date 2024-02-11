import pandas as pd
import sqlite3
import os

import add_ons


# git database absolute path
def initialize():
    db_path = os.path.join(os.path.dirname(__file__), 'database', 'database.db')
    conn = sqlite3.connect(db_path)
    return conn

def list_tables():
    #list all tables in the database
    conn = initialize()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    conn.close()
    table_list = [i[0] for i in tables if i[0] != 'mq_origin']
    return table_list

def add_table(name,dataframe, origin):
    '''Add a table to the database
    name: name of the table
    dataframe: dataframe to be added
    origin: explains how to refresh the data. it will be stored in the mq_origin table in the database 
    '''
    #add a table to the database
    conn = initialize()
    #create table if it doesn't exist otherwise replace entire table with dataframe
    dataframe.to_sql(name, conn, if_exists='replace')
    #add origin to the origin table
    cur = conn.cursor()
    # Check if the mq_origin table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='mq_origin';")
    if not cur.fetchone():
        origin.to_sql('mq_origin', conn, index=False)
    else:
        #grabs origin table from database to check whether the table already exists
        mq_origin = pd.read_sql_query("SELECT * from mq_origin", conn)
        match = mq_origin['table_name'] == origin['table_name'].values[0]
        if match.any():
        # If a match is found, replace that row in the mq_origin DataFrame with the row from the origin DataFrame
            mq_origin.loc[match, :] = origin.values[0]
        else:
        # If no match is found, append the row from the origin DataFrame to the mq_origin DataFrame
            mq_origin = mq_origin._append(origin, ignore_index=True)
        mq_origin.to_sql('mq_origin', conn, index=False, if_exists='replace')

    conn.close()

def list_table_contents(name):
    conn = initialize()

    df = pd.read_sql_query(f"SELECT * FROM {name}", conn)

    conn.close()

    return df

def delete_table(name):
    conn = initialize()
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE {name}")
    #remove table from origin table
    mq_origin = pd.read_sql_query("SELECT * from mq_origin", conn)
    match = mq_origin['table_name'] == name
    mq_origin = mq_origin[~match]
    mq_origin.to_sql('mq_origin', conn, index=False, if_exists='replace')

    conn.close()

def output(format, df, path=None):
    '''Output a dataframe to a file
    format: file format (csv, tsv, or excel)
    df: dataframe to be output
    path: file path to output to
    '''
    if path:
        if format == 'csv':
            df.to_csv(path, index=False)
        elif format == 'tsv':
            df.to_csv(path, sep='\t', index=False)
        elif format == 'excel':
            df.to_excel(path, index=False)
        elif format == 'json':
            df.to_json(path, index=False, orient='records')
        else:
            raise ValueError('Format not supported')
    else:
        if format == 'csv':
            print(df.to_csv(index=False))
        elif format == 'tsv':
            print(df.to_csv(sep='\t', index=False))
        elif format == 'excel':
            print(df.to_excel(index=False))
        elif format == 'json':
            print(df.to_json(index=False, orient='records'))
        else:
            raise ValueError('Format not supported')


def query(query_string):
    '''Query the database
    query_string: SQL query string
    '''
    conn = initialize()
    df = pd.read_sql_query(query_string, conn)
    conn.close()
    return df

def refresh_table(name):
    pass

def list_add_ons():
    addons_directory = os.path.join(os.path.dirname(__file__), "add_ons")
    addons = []

    # Check if the addons directory exists
    if not os.path.exists(addons_directory):
        return 'No add_ons found.'

    # Walk through the directory and look for __init__.py files
    for root, dirs, files in os.walk(addons_directory):
        if '__init__.py' in files:
            addons.append(dirs)

    return '\n'.join(addons[0])

def use_add_on(add_on, args):
    pass


print(list_add_ons())