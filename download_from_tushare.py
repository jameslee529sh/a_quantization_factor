""" 下载tushare提供的股票数据
"""

import tushare as ts

import config

if __name__ == '__main__':
    ts.set_token(config.tushare_token)