""" 下载tushare提供的股票数据
"""
from typing import List, Any, Text, NamedTuple, Tuple, Optional, Callable, Iterator, Dict
from functools import reduce
import sqlite3
from collections import namedtuple
from functools import partial
import time

import tushare as ts
import pandas as pd

from src import config

Sampling_config: NamedTuple = namedtuple('sampling_config', 'start_date, end_date')
DB_config: NamedTuple = namedtuple('db_config', 'db_path, tbl_daily_trading_data, tbl_balance_sheet')


def sampling_config() -> Sampling_config:
    return Sampling_config(start_date='20050430', end_date='20190430')


def db_config() -> DB_config:
    return DB_config(db_path='..\\data\\a_data.db',
                     tbl_daily_trading_data='daily_trading_data',
                     tbl_balance_sheet='balance_sheet')


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
        c.execute(f"SELECT MIN({field_name}), MAX({field_name}) FROM {table_name} WHERE 股票代码='{code}'")
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


def create_gctp_task(code: Text, tbl_name: Text) -> Optional[Tuple]:
    task: Optional[Tuple] = None
    tbl_field: Dict = {db_config().tbl_balance_sheet: ('报告期',), }
    trading_date_range: Tuple = get_extreme_value_in_db(tbl_name, tbl_field[tbl_name][0], code)

    # ToDo: 还需要考虑各种情况，例如，config和db中不一致
    if trading_date_range == (None, None):
        task = (tbl_name, code, sampling_config().start_date, sampling_config().end_date)
    return task


def get_data_from_tushare(task: Tuple) -> Optional[pd.DataFrame]:
    if task is None:
        return None
    get_balance_sheet = lambda: ts.pro_api(config.tushare_token).balancesheet(ts_code=task[1],
                                                                              start_date=task[2], end_date=task[3])
    tbl_tushare = {db_config().tbl_balance_sheet: get_balance_sheet, }

    return tbl_tushare[task[0]]()


def clean_balance_sheet(data: pd.DataFrame) -> pd.DataFrame:
    temp = data.drop_duplicates(['end_date'], keep='first')
    return temp.drop(['ann_date'], axis=1)


def transfer_balance_sheet(data: pd.DataFrame) -> List:
    temp = data.set_index(['ts_code', 'end_date']).reset_index()
    return temp.values.tolist()


def persist_data(data: List, tbl_name: Text) -> Any:
    conn = sqlite3.connect(db_config().db_path)
    c = conn.cursor()

    # Insert list data
    insert_txt: Text = f'INSERT INTO {tbl_name} VALUES ({"?," * 135 + "?"})'
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
         clean_data: Callable[[pd.DataFrame], pd.DataFrame],
         transfer_data: Callable[[pd.DataFrame], List]) -> Optional[bool]:
    data = get_data_from_tushare(create_gctp_task(code, tbl_name))
    return persist_data(transfer_data(clean_data(data)), tbl_name) if data is not None and data.empty is False \
        else None


def limit_access(access_per_minute: int, code: Text, gctp_func: Callable[[Text], Optional[bool]]) -> Any:
    start = time.time()
    rtn = gctp_func(code)
    end = time.time()
    lapse = end - start
    min_lapse = 60 / access_per_minute
    if rtn is not None and lapse <= min_lapse:
        time.sleep(min_lapse - lapse + 0.015)


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

    create_sqlite_table(db_config().tbl_balance_sheet,
                        """股票代码 NOT NULL, 
                        报告期 NOT NULL,
                        实际公告日期 NOT NULL,
                        报表类型 NOT NULL,
                        公司类型,
                        期末总股本,
                        资本公积金,
                        未分配利润,
                        盈余公积金,
                        专项储备,
                        货币资金,
                        交易性金融资产,
                        应收票据,
                        应收账款,
                        其他应收款,
                        预付款项,
                        应收股利,
                        应收利息,
                        存货,
                        待摊费用,
                        一年内到期的非流动资产,
                        结算备付金,
                        拆出资金,
                        应收保费,
                        应收分保账款,
                        应收分保合同准备金,
                        买入返售金融资产,
                        其他流动资产,
                        流动资产合计,
                        可供出售金融资产,
                        持有至到期投资,
                        长期股权投资,
                        投资性房地产,
                        定期存款,
                        其他资产,
                        长期应收款,
                        固定资产,
                        在建工程,
                        工程物资,
                        固定资产清理,
                        生产性生物资产,
                        油气资产,
                        无形资产,
                        研发支出,
                        商誉,
                        长期待摊费用,
                        递延所得税资产,
                        发放贷款及垫款,
                        其他非流动资产,
                        非流动资产合计,
                        现金及存放中央银行款项,
                        存放同业和其它金融机构款项,
                        贵金属,
                        衍生金融资产,
                        应收分保未到期责任准备金,
                        应收分保未决赔款准备金,
                        应收分保寿险责任准备金,
                        应收分保长期健康险责任准备金,
                        存出保证金,
                        保户质押贷款,
                        存出资本保证金,
                        独立账户资产,
                        其中：客户资金存款,
                        其中：客户备付金,
                        其中：交易席位费,
                        应收款项类投资,
                        资产总计,
                        长期借款,
                        短期借款,
                        向中央银行借款,
                        吸收存款及同业存放,
                        拆入资金,
                        交易性金融负债,
                        应付票据,
                        应付账款,
                        预收款项,
                        卖出回购金融资产款,
                        应付手续费及佣金,
                        应付职工薪酬,
                        应交税费,
                        应付利息,
                        应付股利,
                        其他应付款,
                        预提费用,
                        递延收益,
                        应付短期债券,
                        应付分保账款,
                        保险合同准备金,
                        代理买卖证券款,
                        代理承销证券款,
                        一年内到期的非流动负债,
                        其他流动负债,
                        流动负债合计,
                        应付债券,
                        长期应付款,
                        专项应付款,
                        预计负债,
                        递延所得税负债,
                        递延收益——非流动负债,
                        其他非流动负债,
                        非流动负债合计,
                        同业和其它金融机构存放款项,
                        衍生金融负债,
                        吸收存款,
                        代理业务负债,
                        其他负债,
                        预收保费,
                        存入保证金,
                        保户储金及投资款,
                        未到期责任准备金,
                        未决赔款准备金,
                        寿险责任准备金,
                        长期健康险责任准备金,
                        独立账户负债,
                        其中：质押借款,
                        应付赔付款,
                        应付保单红利,
                        负债合计,
                        减：库存股,
                        一般风险准备,
                        外币报表折算差额,
                        未确认的投资损失,
                        少数股东权益,
                        股东权益合计（不含少数股东权益）,
                        股东权益合计（含少数股东权益）,
                        负债及股东权益总计,
                        长期应付职工薪酬,
                        其他综合收益,
                        其他权益工具,
                        其他权益工具（优先股）,
                        融出资金,
                        应收款项,
                        应付短期融资款,
                        应付款项,
                        持有待售的资产,
                        持有待售的负债,
                        PRIMARY KEY (股票代码, 报告期)""")

    tscode_iter: Iterator[Text] = (record[0] for record in download_list_companies().values.tolist())

    gctp_balance_sheet: Callable[[Text], Optional[bool]] = partial(gctp, tbl_name=db_config().tbl_balance_sheet,
                                 clean_data=clean_balance_sheet, transfer_data=transfer_balance_sheet)

    for ts_code in tscode_iter:
        #     gctp_daily_trading_data(ts_code)
        limit_access(80, ts_code, gctp_balance_sheet)
