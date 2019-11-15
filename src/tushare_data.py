""" 下载tushare提供的股票数据
"""
from typing import List, Any, Text, NamedTuple, Tuple, Optional
from functools import reduce
import sqlite3
from collections import namedtuple

import tushare as ts
import pandas as pd

from src import config

Sampling_config: NamedTuple = namedtuple('sampling_config', 'start_date, end_date')
DB_config: NamedTuple = namedtuple('db_config', 'db_path, tbl_daily_trading_data')


def sampling_config() -> Sampling_config:
    return Sampling_config(start_date='20050430', end_date='20190430')


def db_config() -> DB_config:
    return DB_config(db_path='..\\data\\a_data.db', tbl_daily_trading_data='daily_trading_data')


def download_list_companies() -> pd.DataFrame:
    download = lambda status: ts.pro_api(config.tushare_token).\
        stock_basic(exchange='', list_status=status, fields='ts_code, symbol,name,area,industry,list_date, delist_date')
    list_companies = [download(s) for s in ['L', 'D', 'P']]
    return reduce(lambda x, y: x.append(y, ignore_index=True), list_companies)


def persist_list_companies_to_db(list_companies: List):
    conn = sqlite3.connect('..\\data\\a_data.db')
    c = conn.cursor()

    # Create table
    c.execute('''CREATE TABLE IF NOT EXISTS \
     list (代码 text, 名称 text, 地域 text, 行业 text, 上市日期 text, 退市日期 text)''')

    # Insert list data
    c.executemany('INSERT INTO list VALUES (?,?,?,?,?,?)', list_companies)

    # Save (commit) the changes
    conn.commit()

    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()


def transfer_list_companies(list_companies: pd.DataFrame) -> List:
    return [record[1:] for record in list_companies[0].values.tolist()]


def get_process_tushare_list() -> bool:
    persist_list_companies_to_db(transfer_list_companies(download_list_companies()))
    return True


def create_sqlite_table(table_name: Text, column_def: Text) -> Any:
    conn = sqlite3.connect('..\\data\\a_data.db')
    c = conn.cursor()
    c.execute(f'''CREATE TABLE IF NOT EXISTS {table_name} ({column_def})''')
    conn.commit()
    conn.close()


def get_extreme_value_in_db(table_name: Text, field_name: Text, code: Text) -> Tuple:
    trading_date_range: Tuple = (None,)
    conn = sqlite3.connect(db_config().db_path)
    c = conn.cursor()
    try:
        c.execute(f"SELECT MAX({field_name}), MIN({field_name}) FROM {table_name} WHERE 股票代码='{code}'")
        trading_date_range = c.fetchone()
    except sqlite3.Error as e:
        print(e)
    finally:
        c.close()
        conn.close()
    return trading_date_range


def create_daily_trading_data_task(code: Text) -> Optional[Tuple]:
    trading_date_range: Tuple = get_extreme_value_in_db(db_config().tbl_daily_trading_data, '交易日期', code)
    task: Optional[Tuple] = None

    # ToDo: 还需要考虑各种情况，例如，config和db中不一致
    if trading_date_range == (None, None):
        task = (code, sampling_config().start_date, sampling_config().end_date)
    return task


def get_tushare_data(task: Optional[Tuple]) -> Optional[pd.DataFrame]:
    return ts.pro_bar(ts_code=task[0], api=ts.pro_api(config.tushare_token), start_date=task[1], end_date=task[2],
                      adj='qfq', factors=['tor', 'vr'], adjfactor=True) if task is not None else None


# drop some columns
def clean_daily_trading_data(data: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    return data.drop(['pre_close', 'change', 'pct_chg'], axis=1) if data is not None else None


# reindex dataframe and transfer dataframe to list
def transfer_daily_trading_data(data: Optional[pd.DataFrame]) -> Optional[List]:
    temp = data.reindex(columns=['code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount',
                                 'turnover_rate', 'volume_ratio', 'adj_factor']) if data is not None else None
    return temp.values.tolist() if temp is not None else None


def persist_daily_trading_data(data: List) -> Any:
    if data is None:
        return
    conn = sqlite3.connect(db_config().db_path)
    c = conn.cursor()

    # Insert list data
    c.executemany(f'INSERT INTO {db_config().tbl_daily_trading_data} VALUES (?,?,?,?,?,?,?,?,?,?,?)', data)

    # Save (commit) the changes
    conn.commit()

    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    c.close()
    conn.close()


# get, clean, transfer and persist data ==> gctp
def gctp_daily_trading_data(code: Text) -> Any:
    return persist_daily_trading_data(
        transfer_daily_trading_data(
            clean_daily_trading_data(
                get_tushare_data(
                    create_daily_trading_data_task(code)))))


if __name__ == '__main__':
    # persist_list_companies_to_db(transfer_list_companies(download_list_companies()))
    create_sqlite_table('daily_trading_data',
                        """股票代码 NOT NULL, 
                        交易日期 NOT NULL, 
                        开盘价 NOT NULL,
                        最高价 NOT NULL,
                        最低价 NOT NULL,
                        收盘价 NOT NULL,
                        成交量 NOT NULL,
                        成交额 NOT NULL,
                        换手率 NOT NULL,
                        量比,
                        复权因子 NOT NULL,
                        PRIMARY KEY (股票代码, 交易日期)""")
    tscode_list: List[Text] = [record[0] for record in download_list_companies().values.tolist()]

    for ts_code in tscode_list:
        gctp_daily_trading_data(ts_code)
