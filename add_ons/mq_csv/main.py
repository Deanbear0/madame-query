import argparse
import os
import pandas as pd
import sqlite3
import json

import sys

# Add the grandparent directory to the system path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import base

class argument:
    def __init__(self, path, name):
        self.path = path
        self.name = name
def main(args):
    #checks to see if args.path and args.name are valid
    path_index = args.index('--path')
    name_index = args.index('--name')
    if path_index == -1:
        path = input('Enter the path of the csv file: ')
    else:
        path = args[path_index + 1]
    if name_index == -1:
        name = input('Enter the name of the table: ')
    else:
        name = args[name_index + 1]
    
    #checks to see if the file exists
    path = os.path.expanduser(path)
    if not os.path.exists(path):
        #create result dataframe
        result = pd.DataFrame(columns=['result'], data=[f' Error File not found at {path}'])
        return result
    #check if file is csv
    if not path.endswith('.csv'):
        #create result dataframe
        result = pd.DataFrame(columns=['result'], data=[' Error File format is not csv.'])
        return result
    #convert csv to dataframe
    df = pd.read_csv(path)
    #use use base to create table
    base.add_table(dataframe=df, name=name, origin=gen_origin(name, path))
    result = pd.DataFrame(columns=['result'], data=[f'Success Table {name} created'])
    return result

def gen_origin(name, path):
    params = json.dumps({'path': path})
    df = {'table_name':[name], 'add_on':['mq_csv'], 'params':[params]}
    dataframe = pd.DataFrame(df)
    return dataframe

if __name__ == '__main__':
    args = argument('~/Downloads/test\ -\ Sheet1.csv', 'test')
    main(args=args)

