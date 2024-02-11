import pandas as pd
import sqlite3
import os
import argparse


import base
import add_ons



def main():
    parser = argparse.ArgumentParser(description='make queries to csv,tsv, json, or excel files', proq='madame query')
    parser.add_argument('query', type=str, help='SQL query string')
    parser.add_argument('ls', type=str, help='list all tables in the database')
    parser.add_argurment('rm', type=str, help='delete table add the table name to delete')
    parser.add_argument('refresh', type=str, help='whatever table you specify will be refreshed')
    parser.add_argument('-f','--format', type=str, help='file format you want the output to be (csv, tsv, or excel)')
    parser.add_argument('-o','--output', type=str, help='file path to output to')
    parser.add_argument('-a','--add-ons', type=str, help='add-on to use')
    




