import pandas as pd
import sqlite3
import os

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
    table_list = [i[0] for i in tables]
    return table_list

def add_table(name,dataframe, origin):
    '''Add a table to the database
    name: name of the table
    dataframe: dataframe to be added
    origin: explains how to refresh the data. it will be stored in the querist_origin table in the database 
    '''
    #add a table to the database
    conn = initialize()
    #create table if it doesn't exist otherwise replace entire table with dataframe
    dataframe.to_sql(name, conn, if_exists='replace')
    #adds origin to the quirist_origin table
    querist_origin = pd.read_sql_query("SELECT * FROM querist_origin", conn)
    

    conn.close()

def list_table_contents(name):
    conn = initialize()

    df = pd.read_sql_query(f"SELECT * FROM {name}", conn)

    conn.close()

    return df
    


print(list_tables())

#create a test dataframe
df = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]})
origin = pd.DataFrame({'table_name': ['test'],'add-on':['csv'] , 'origin': ['test']})
add_table('test', df, origin)
#add another table
df = pd.DataFrame({'c': [7,8,9], 'd': [10,11,12]})
origin = pd.DataFrame({'table_name': ['test2'],'add-on':['csv'] , 'origin': ['test2']})
add_table('test2', df, origin)



print(list_tables())
print(list_table_contents('test'))
print(list_table_contents('querist_origin'))