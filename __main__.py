import pandas as pd
import sqlite3
import os
import argparse


import base
import add_ons



def main():
    parser = argparse.ArgumentParser(description='make queries to csv,tsv, json, or excel files', prog='madame query')
    parser.add_argument('--query', type=str, help='SQL query string')
    parser.add_argument('-lt','--list_table', help='list all tables in the database if -t --table is used it will print the table contents', action='store_true')
    parser.add_argument('--list_add_ons','-la', help='lists all add-ons installed', action='store_true')
    parser.add_argument('-rm','--remove_table', help='delete table add the table name to delete', action='store_true')
    parser.add_argument('--refresh','-rf', help='whatever table you specify will be refreshed', action='store_true')
    parser.add_argument('-f','--format', type=str, help='file format you want the output to be (csv, tsv, or excel)')
    parser.add_argument('-o','--output', type=str, help='file path to output to')
    parser.add_argument('-a','--add_on', type=str, help='add-on to use')
    parser.add_argument('-t','--table', type=str, help='table name to use')



    args, params = parser.parse_known_args()
    if args.list_table and not args.table:
        output(base.list_tables(), args.format, args.output)
    elif args.list_table and args.table:
        output(base.query(f"SELECT * FROM {args.table}"), args.format, args.output)
    if args.list_add_ons:
        output(base.list_add_ons(), args.format, args.output)

    if args.remove_table:
        if not args.table:
            args.table = input('Enter table name to delete: ')
        output(base.delete_table(args.table), args.format, args.output)
    if args.refresh:
        if not args.table:
            args.table = input('Enter table name to refresh: ')
        base.refresh_table(args.table)
    if args.query:
        output(base.query(args.query), args.format, args.output)
    if args.add_on:
        output(base.run_add_on(add_on=args.add_on, args = params), args.format, args.output)

        

        


def output(df, format, path):
    '''Output the dataframe to a file
    df: dataframe
    format: file format
    path: file path
    '''
    if not format:
        print(df.to_string(index=False))
    else:
        base.output(format, df, path)

if __name__ == '__main__':
    main()

    




