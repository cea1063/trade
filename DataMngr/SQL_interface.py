# coding: utf-8

import sqlite3
import pandas as pd
from Util.Constant import *

file_name = __file__.split('/')[-1]


def get_all_table_name(db_name):
    with sqlite3.connect('{}.db'.format(db_name)) as con:
        cur = con.cursor()
        cur.execute('SELECT name FROM sqlite_master WHERE type="table";')
        return cur.fetchall()


def delete_table(db_name, table_name):
    with sqlite3.connect('{}.db'.format(db_name)) as con:
        cur = con.cursor()
        cur.execute('DROP TABLE {}'.format(table_name))
        return con.commit()


def get_df_from_table(db_name, table_name):
    with sqlite3.connect('{}.db'.format(db_name)) as con:
        return pd.read_sql('SELECT * FROM {}'.format(table_name), con)


def get_last_updated_date(db_name, table_name):
    with sqlite3.connect('{}.db'.format(db_name)) as con:
        cur = con.cursor()
        sql = "SELECT 날짜 FROM {} ORDER BY ROWID DESC LIMIT 1".format(table_name)
        cur.execute(sql)
        result = cur.fetchone()
        return result[0]


def check_table_exist(db_name, table_name):
    with sqlite3.connect('{}.db'.format(db_name)) as con:
        cur = con.cursor()
        sql = "SELECT name FROM sqlite_master WHERE type='table' and name=:table_name"
        cur.execute(sql, {"table_name": table_name})

        if len(cur.fetchall()) > 0:
            return True
        else:
            return False


def change_table_name(db_name, before, after):
    with sqlite3.connect('{}.db'.format(db_name)) as con:
        cur = con.cursor()
        sql = ("ALTER TABLE '{}' RENAME TO {}".format(before, after))
        cur.execute(sql)


def insert_df_to_db(db_name, table_name, df, option="replace"):
    with sqlite3.connect('{}.db'.format(db_name)) as con:
        df.to_sql(table_name, con, if_exists=option)


def sort_df_by_date(db_name, table_name):
    with sqlite3.connect('{}.db'.format(db_name)) as con:
        df = pd.read_sql('SELECT * FROM {}'.format(table_name), con)
        df.drop_duplicates(inplace=True)
        df.sort_values(by=[DAY], inplace=True)
        df.set_index(keys=[DAY], inplace=True)
        df.to_sql(table_name, con, if_exists='replace')


def execute_sql(db_name, sql, param={}):
    with sqlite3.connect('{}.db'.format(db_name)) as con:
        cur = con.cursor()
        cur.execute(sql, param)
        return cur

