#!/home/bryan.yang/miniconda2/bin/python
from __future__ import print_function
from __future__ import division
import sys
sys.path.append("/home/bryan.yang/opt/ml-tools")
from mltools import fit
import argparse
import pandas as pd
import datetime
import subprocess

def run(method):
    def print_args(*args, **kwargs):
        print('ml-tool: %s' % method.__name__)
        print(kwargs)
        src = kwargs.get('src')
        col = kwargs.get('col')
        name = kwargs.get('name')
        dbname = kwargs.get('dbname')
        table = kwargs.get('table')
        frac= kwargs.get('sample', 1)

        df = pd.read_csv(src, sep='\t')
        df = df.sample(frac=frac)
        print("number of rows: %s" % (df.ix[:,0].count()))
        cv = method(df, col, kwargs)
        print("critical value: %s" % cv)
        insert_table(dbname, table, name, cv)
    return print_args

def insert_table(dbname, table, name, cv):
    txdate = datetime.datetime.now().strftime("%Y-%m-%d")
    impala_args = ['impala-shell']
    sql_string = "insert into table %s.%s values ('%s', %s, '%s')" % (dbname, table, name, cv, txdate)
    print('sql string: %s' % sql_string)
    impala_args.append('-q')
    impala_args.append(sql_string)
    subprocess.call(impala_args)

@run
def weibull(df, col, kwargs):
    cp = kwargs.get('cp')
    cv = fit.get_cv(pd.DataFrame(df[col]), 'wei', cp=cp)
    return cv

@run 
def gaussian(df, col, kwargs):
    cp = kwargs.get('cp')
    cv = fit.get_cv(df[col], 'norm', cp=cp)
    return cv

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=run)
    parser.add_argument('--src', type=str, required=True, metavar='source file')
    parser.add_argument('--col', type=str, required=True, metavar='column name')
    parser.add_argument('--name', type=str, required=True, metavar='threshold name')
    parser.add_argument('--dbname', type=str, required=True, metavar='target db')
    parser.add_argument('--table', type=str, required=True, metavar='target table')
    parser.add_argument('--sample', type=float, choices=[0.1, 0.01, 0.001], default=argparse.SUPPRESS, metavar='sample rate')


    subparsers = parser.add_subparsers(title="ml-tools")
    parser_weibull = subparsers.add_parser('weibull')
    parser_weibull.add_argument("cp", type=float, nargs='?', default=0.9999)
    parser_weibull.set_defaults(func=weibull)

    parser_gaussian = subparsers.add_parser('gaussian')
    parser_gaussian.add_argument("cp", type=float, nargs='?', default=0.9999)
    parser_gaussian.set_defaults(func=gaussian)

    parser_foo = subparsers.add_parser('foo')
    parser_foo.add_argument("-a", type=int, default=101)

    args = parser.parse_args()
    args.func(**vars(args))
