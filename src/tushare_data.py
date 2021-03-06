""" 下载tushare提供的股票数据
"""
from typing import List, Any, Text, NamedTuple, Tuple, Optional, Callable, Dict, Iterator
from functools import reduce, lru_cache
import sqlite3
from collections import namedtuple
import time
import datetime

import tushare as ts
import pandas as pd

from src import config

Sampling_config: NamedTuple = namedtuple('sampling_config', 'start_date, end_date')
DB_config: NamedTuple = namedtuple('db_config', "db_path, tbl_daily_trading_data, tbl_balance_sheet, \
        tbl_income_statement, tbl_cash_flow_statement, tbl_finance_indicator_statement, tbl_daily_basic, \
        tbl_index, tbl_name_history")


def sampling_config() -> Sampling_config:
    return Sampling_config(start_date='20050430', end_date='20190430')


def db_config() -> DB_config:
    return DB_config(db_path='..\\data\\a_data.db',
                     tbl_daily_trading_data='daily_trading_data',
                     tbl_balance_sheet='balance_sheet',
                     tbl_income_statement='income_statement',
                     tbl_cash_flow_statement='cash_flow_statement',
                     tbl_finance_indicator_statement='finance_indicator',
                     tbl_daily_basic='daily_basic',
                     tbl_index='securities_index',
                     tbl_name_history='name_history')


@lru_cache(128)
def download_list_companies(call_date: datetime.datetime = datetime.datetime.now()) -> pd.DataFrame:
    download = lambda status: ts.pro_api(config.tushare_token).\
        stock_basic(exchange='', list_status=status, fields='ts_code, symbol,name,area,industry,list_date, delist_date')
    list_companies = [download(s) for s in ['L', 'D', 'P']]
    return reduce(lambda x, y: x.append(y, ignore_index=True), list_companies)


def imp_create_sqlite_table(table_name: Text, column_def: Text) -> Any:
    conn = sqlite3.connect('..\\data\\a_data.db')
    c = conn.cursor()
    c.execute(f'''CREATE TABLE IF NOT EXISTS {table_name} ({column_def})''')
    conn.commit()
    conn.close()


def imp_get_extreme_value_in_db(table_name: Text, field_name: Text, code: Text) -> Tuple:
    trading_date_range: Tuple = (None,)
    conn = sqlite3.connect(db_config().db_path)
    c = conn.cursor()
    try:
        c.execute(f"SELECT MIN({field_name}), MAX({field_name}) FROM {table_name} WHERE ts_code='{code}'")
        trading_date_range = c.fetchone()
    except sqlite3.Error as e:
        print(e)
    finally:
        c.close()
        conn.close()
    return trading_date_range


def imp_get_records_from_db(sql_str: Text) -> Optional[pd.DataFrame]:
    conn = sqlite3.connect(db_config().db_path)
    c = conn.cursor()
    records = None
    names = None
    try:
        c.execute(sql_str)
        names = [description[0] for description in c.description]
        records = c.fetchall()
    except sqlite3.Error as e:
        print(e)
    finally:
        c.close()
        conn.close()
    return db_tuples_to_dataframe(records, names) if names is not None else None


def db_tuples_to_dataframe(data: List[Tuple], names: List[Text]) -> pd.DataFrame:
    SQLiteTable = namedtuple('sqlitetable', names)
    return pd.DataFrame([SQLiteTable(*d) for d in data])


def create_gctp_task(code: Text, tbl_name: Text) -> Optional[Tuple]:
    task: Optional[Tuple] = None
    field_name: Text = 'trade_date' \
        if tbl_name == db_config().tbl_daily_trading_data or tbl_name == db_config().tbl_daily_basic \
           or tbl_name == db_config().tbl_index \
        else 'end_date'
    trading_date_range: Tuple = imp_get_extreme_value_in_db(tbl_name, field_name, code)

    # ToDo: 还需要考虑各种情况，例如，config和db中不一致
    if trading_date_range == (None, None):
        task = (tbl_name, code, sampling_config().start_date, sampling_config().end_date)
    return task


def imp_get_data_from_tushare(task: Tuple) -> Optional[pd.DataFrame]:
    if task is None:
        return None

    ts.set_token(config.tushare_token)
    func: Dict = {db_config().tbl_finance_indicator_statement: ts.pro_api().fina_indicator,
                  db_config().tbl_income_statement: ts.pro_api().income,
                  db_config().tbl_balance_sheet: ts.pro_api().balancesheet,
                  db_config().tbl_cash_flow_statement: ts.pro_api().cashflow,
                  db_config().tbl_daily_basic: ts.pro_api().daily_basic}

    tbl_name = task[0]
    if tbl_name in func:
        return func[tbl_name](ts_code=task[1], start_date=task[2], end_date=task[3])
    elif tbl_name == db_config().tbl_daily_trading_data:
        return ts.pro_bar(ts_code=task[1], start_date=task[2], end_date=task[3], adj='qfq')
    elif tbl_name == db_config().tbl_name_history:
        return ts.pro_api().namechange(ts_code=task[1])
    else:
        return None


def imp_get_index_daily_basic_from_tushare(ts_code: Text, trade_date: Text) -> NamedTuple:
    df: pd.DataFrame = ts.pro_api(config.tushare_token).index_dailybasic(ts_code=ts_code, trade_date=trade_date)
    return list(df.itertuples(index=False, name='index_daily_basic'))[0]


def imp_get_trade_data_from_tushare(task: Tuple) -> Optional[pd.DataFrame]:
    ts.set_token(config.tushare_token)
    asset: Dict = {db_config().tbl_daily_trading_data: 'E',
                   db_config().tbl_index: 'I'}
    return ts.pro_bar(ts_code=task[1], asset=asset[task[0]], start_date=task[2], end_date=task[3], adj='qfq') \
        if task is not None else None


def clean_statement2(data: pd.DataFrame) -> pd.DataFrame:
    return data.drop_duplicates(['end_date'], keep='first') if 'end_date' in list(data.columns.values) else data


def transfer_statement(data: pd.DataFrame) -> List:
    return list(data.values)


def imp_persist_data(data: List, tbl_name: Text) -> Any:
    conn = sqlite3.connect(db_config().db_path)
    c = conn.cursor()

    # Insert list data
    fields_len: int = len(data[0])
    insert_txt: Text = f'INSERT OR IGNORE INTO {tbl_name} VALUES ({"?," * (fields_len - 1) + "?"})'
    c.executemany(insert_txt, data)

    # Save (commit) the changes
    conn.commit()

    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    c.close()
    conn.close()
    return True


# get, clean, transfer and persist data, gctp
def gctp(code: Text, tbl_name: Text,
         getter: Callable[[Tuple], Any],
         persistence: Callable[[pd.DataFrame, Text], Any]) -> Optional[bool]:
    data = getter((tbl_name, code, sampling_config().start_date, sampling_config().end_date))
    return persistence(transfer_statement(clean_statement2(data)), tbl_name) \
        if data is not None and data.empty is False else None


def impf_gctp_daily_trade_data(ts_code: Text) -> Optional[Text]:
    rtn: Text = (ts_code, f"日交易数据没有成功缓存到本地 ")

    # TODO: 数据是否已经保存在数据库中
    range_in_db: Tuple = imp_get_extreme_value_in_db(db_config().tbl_daily_trading_data, 'trade_date', ts_code)

    df: pd.DataFrame = ts.pro_api(config.tushare_token).daily(ts_code=ts_code,
                                                              start_date=sampling_config().start_date,
                                                              end_date=sampling_config().end_date)

    if df is not None and df.empty is not True:
        # TODO: 避免重复插入
        imp_persist_data(list(df.values), db_config().tbl_daily_trading_data)
        rtn = (ts_code, f"日交易数据缓存到本地数据库的{db_config().tbl_daily_trading_data}表格成功")

    return rtn


def impf_name_is_in_db(ts_code: Text) -> bool:
    rtn: bool = False
    df: pd.DataFrame = imp_get_records_from_db(f"SELECT * FROM name_history WHERE ts_code='{ts_code}'")
    if df is not None and df.empty is False:
        rtn = True
    return rtn


def impf_gctp_name_history(ts_code: Text) -> Optional[Text]:
    rtn: Text = (ts_code, f"股票名称信息没有成功缓存到本地 ")

    df: pd.DataFrame = ts.pro_api(config.tushare_token).namechange(ts_code=ts_code)
    if df is not None and df.empty is not True:
        # TODO: 避免重复插入
        imp_persist_data(list(df.values), db_config().tbl_name_history)
        rtn = (ts_code, f"股票名称信息缓存到本地数据库的{db_config().tbl_name_history}表格成功")

    return rtn


def imp_limit_access(access_per_minute: int,
                     code_set: List,
                     gctp_func: Callable[[Text], Optional[bool]],
                     exists_in_db: Optional[Callable[[Text], bool]] = None) -> Any:
    for code in code_set:
        if exists_in_db is not None and exists_in_db(code) is True:
            continue

        start = time.time()
        rtn = gctp_func(code)
        end = time.time()
        lapse = end - start
        min_lapse = 60 / access_per_minute
        if lapse <= min_lapse:
            time.sleep(min_lapse - lapse + 0.002)
        print(rtn)


# TODO: daily_trading_data table需要建立trade_date INDEX

def imp_create_fina_tables() -> Optional:
    tbl_list: List[Text] = [db_config().tbl_cash_flow_statement, db_config().tbl_balance_sheet,
                            db_config().tbl_income_statement, db_config().tbl_finance_indicator_statement,
                            db_config().tbl_daily_basic, db_config().tbl_daily_trading_data]
    for item in tbl_list:
        df: pd.DataFrame = imp_get_data_from_tushare((item, '600000.SH', '20170101', '20180801'))
        query_str = reduce(lambda x, y: f"{x}, {y}", df.columns.to_list())
        query_str = query_str + ", PRIMARY KEY (ts_code, end_date)" \
            if item != db_config().tbl_daily_trading_data and item != db_config().tbl_daily_basic \
            else query_str + ", PRIMARY KEY (ts_code, trade_date)"
        imp_create_sqlite_table(item, query_str)


def imp_create_trade_tables() -> Optional:
    tbl_list: List[Text] = [db_config().tbl_index, ]

    for item in tbl_list:
        sample_code: Callable[[Text], Text] = lambda t: '399300.SZ' if t == db_config().tbl_index else '600000.SH'
        df: pd.DataFrame = imp_get_trade_data_from_tushare((item, sample_code(item), '20170101', '20170301'))
        query_str = reduce(lambda x, y: f"{x}, {y}", df.columns.to_list())
        query_str = query_str + ", PRIMARY KEY (ts_code, trade_date)"
        imp_create_sqlite_table(item, query_str)


def create_sqlite_table(sample: pd.DataFrame,
                        tbl_name: Text,
                        primary_keys: Text) -> Optional[bool]:
    query_str = reduce(lambda x, y: f"{x}, {y}", sample.columns.to_list()) + f", PRIMARY KEY ({primary_keys})"
    imp_create_sqlite_table(tbl_name, query_str)


def create_db_tables(tbl_name: Text,
                     build_tbl_sql_func: Callable[[Text], Text],
                     create_table_func: Callable[[Text, Text], Any]) -> Any:
    create_table_func(tbl_name, build_tbl_sql_func(tbl_name))


def imp_get_trade_cal(start: Text, end: Text) -> Iterator[Tuple[Text, int]]:
    return ts.pro_api(config.tushare_token).trade_cal(exchange='', start_date=start, end_date=end)\
        .itertuples(index=False, name='Trade_cal')


if __name__ == '__main__':
    # create_db_tables(db_config().tbl_name_history,
    #                  lambda s: 'ts_code, name, start_date, end_date, ann_date, change_reason \
    #                             PRIMARY KEY (ts_code, start_date) ',
    #                  imp_create_sqlite_table)
    # imp_create_fina_tables()
    # imp_create_trade_tables()
    create_sqlite_table(ts.pro_api(config.tushare_token).namechange(ts_code='600848.SH'),
                        tbl_name='name_history',
                        primary_keys='ts_code, start_date')
    code_list: List = [record[0] for record in list(download_list_companies().values)]
    # imp_limit_access(80, code_set=code_list, gctp_func=partial(gctp2,
    #                                                            tbl_name=db_config().tbl_finance_indicator_statement,
    #                                                            getter=imp_get_data_from_tushare,
    #                                                            persistence=imp_persist_data))
    # imp_limit_access(80, code_set=code_list, gctp_func=partial(gctp2,
    #                                                            tbl_name=db_config().tbl_income_statement,
    #                                                            getter=imp_get_data_from_tushare,
    #                                                            persistence=imp_persist_data))
    # imp_limit_access(80, code_set=code_list, gctp_func=partial(gctp2,
    #                                                            tbl_name=db_config().tbl_balance_sheet,
    #                                                            getter=imp_get_data_from_tushare,
    #                                                            persistence=imp_persist_data))
    # imp_limit_access(80, code_set=code_list, gctp_func=partial(gctp,
    #                                                            tbl_name=db_config().tbl_daily_trading_data,
    #                                                            getter=imp_get_data_from_tushare,
    #                                                            persistence=imp_persist_data))
    # imp_limit_access(80, code_set=code_list, gctp_func=partial(gctp,
    #                                                            tbl_name=db_config().tbl_daily_basic,
    #                                                            getter=imp_get_data_from_tushare,
    #                                                            persistence=imp_persist_data))
    # imp_limit_access(80, code_set=['399300.SZ', ], gctp_func=partial(gctp,
    #                                                                  tbl_name=db_config().tbl_index,
    #                                                                  getter=imp_get_trade_data_from_tushare,
    #                                                                  persistence=imp_persist_data))
    # imp_limit_access(100, code_set=code_list, gctp_func=partial(gctp,
    #                                                            tbl_name=db_config().tbl_name_history,
    #                                                            getter=imp_get_data_from_tushare,
    #                                                            persistence=imp_persist_data))
    # imp_limit_access(200, code_set=['600018.SH', ], gctp_func=impf_gctp_daily_trade_data)
    imp_limit_access(78, code_set=code_list, gctp_func=impf_gctp_name_history, exists_in_db=impf_name_is_in_db)
