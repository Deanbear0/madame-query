import pandas as pd
import sqlite3
import os
import sys
import importlib

import add_ons

# git database absolute path
def initialize():
    db_path = os.path.join(os.path.dirname(__file__), 'database', 'database.db')
    conn = sqlite3.connect(db_path)
    return conn

def list_tables():
    #list all tables in the database
    conn = initialize()
    #creat datadrame of all tables in the database
    table_list = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
    #remove mq_origin table from the list
    table_list = table_list[table_list['name'] != 'mq_origin']
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

    results = pd.DataFrame(columns=['result'], data=[f'Table {name} deleted'])
    return results

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
    add_ons = [i for i in addons[0] if i != '__pycache__']
    #turn add_ons to dataframe
    add_ons = pd.DataFrame(add_ons, columns=['add_ons'])

    return add_ons

def run_add_on(add_on, args):
    # Check if the add_on exists in list list add ons dataframe
    listAddOn = list_add_ons()
    if add_on not in listAddOn['add_ons'].values:
        return f'Add-on {add_on} not found.'

    # Import the add-on
    add_on_module = importlib.import_module(f'add_ons.{add_on}.main')

    # Run the add-on
    return add_on_module.main(args)

def refresh_table(name):
    '''Refresh a table in the database
    name: name of the table to be refreshed
    '''
    conn = initialize()
    #grabs origin table from database to check whether the table already exists
    mq_origin = pd.read_sql_query(f"SELECT * from mq_origin", conn)
    origin = mq_origin[mq_origin['table_name'] == name]
    if origin.empty:
        return 'Table not found'
    else:
        add_on = origin['add_on'].values[0]
        params = origin['params'].values[0]
     # Check if the add_on exists
    if  name not in list_add_ons():
        return f'Add-on {add_on} not found.'

    # Import the add-on
    add_on_module = importlib.import_module(f'add_ons.{name}.refresh')

    # Run the add-on
    return add_on_module.main(name, params)



#pulls the add-on from a git repository and adds it to the add-ons directory     
def pull_add_on(url):
    #checks if it is a git repository
    if not url.endswith('.git'):
        return 'Not a git repository'
    #cds into the add_ons dir and pulls the git repository
    add_ons_dir = os.path.join(os.path.dirname(__file__), 'add_ons')
    os.system(f'cd {add_ons_dir} && git clone {url}')
    #adds the add-on to the __init__.py file
    with open(os.path.join(add_ons_dir, '__init__.py'), 'a') as f:
        f.write(f"from . import {url.split('/')[-1].split('.')[0]}")
    return f"Add-on {url.split('/')[-1].split('.')[0]} added successfully"

#removes an add-on from the add-ons directory
def remove_add_on(add_on):
    #checks if the add-on exists
    if add_on not in list_add_ons():
        return f'Add-on {add_on} not found.'
    #removes the add-on from the __init__.py file
    add_ons_dir = os.path.join(os.path.dirname(__file__), 'add_ons')
    with open(os.path.join(add_ons_dir, '__init__.py'), 'r') as f:
        lines = f.readlines()
    with open(os.path.join(add_ons_dir, '__init__.py'), 'w') as f:
        for line in lines:
            if f'from . import {add_on}' not in line:
                f.write(line)
    #removes the add-on from the add-ons directory
    os.system(f'rm -rf {os.path.join(add_ons_dir, add_on)}')
    return f"Add-on {add_on} removed successfully"

#updates an add-on in the add-ons directory
def update_add_on(add_on):
    #checks if the add-on exists
    if add_on not in list_add_ons():
        return f'Add-on {add_on} not found.'
    #cds into the add-on directory and pulls the git repository
    add_ons_dir = os.path.join(os.path.dirname(__file__), 'add_ons')
    os.system(f'cd {os.path.join(add_ons_dir, add_on)} && git pull')
    return f"Add-on {add_on} updated successfully"



if __name__ == '__main__':
    print(run_add_on('mq_csv', None))