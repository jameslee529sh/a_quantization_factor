""" 下载tushare提供的股票数据
"""
from typing import Tuple
from functools import reduce

import tushare as ts
import pandas as pd

import config   # 参考example.config.py, 码友可自行撰写config.py

Download_Result = Tuple[pd.DataFrame, int]


def download_list_companies() -> Download_Result:
    download = lambda status: ts.pro_api(config.tushare_token).\
        stock_basic(exchange='', list_status=status, fields='symbol,name,area,industry,list_date, delist_date')
    list_companies = [download(s) for s in ['L', 'D', 'P']]
    return reduce(lambda x, y: x.append(y, ignore_index=True), list_companies), 3


if __name__ == '__main__':
    print(download_list_companies())