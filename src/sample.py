""" 构建用于因子研究的股票样本数据池
"""
from typing import NamedTuple, Iterator, Text, Tuple, Callable
from collections import namedtuple

from src import tushare_data as td

Sample_config: NamedTuple = namedtuple('sample_config', 'start_date, end_date, updated_date1, updated_date2')


def sample_config() -> Sample_config:
    return Sample_config(start_date='20050430', end_date='20190430', updated_date1='0430', updated_date2='1031')


def filter_updated_date(trade_cal_iter: Iterator[Tuple[Text, Text]]) -> Iterator[Text]:
    test_updated_date: Callable[[Text], bool] = lambda d: d.find(sample_config().updated_date1) == 4 \
                                                       or d.find(sample_config().updated_date2) == 4
    for exchange, cal_date, is_open in trade_cal_iter:
        if test_updated_date(cal_date) is True:
            if is_open == 1:
                test_updated_date = lambda d: d.find(sample_config().updated_date1) == 4 \
                                           or d.find(sample_config().updated_date2) == 4
                yield cal_date
            else:
                test_updated_date = lambda d: True if is_open == 1 else False


if __name__ == "__main__":
    open_iter: Iterator[Text] = filter_updated_date(td.imp_get_trade_cal(start=sample_config().start_date,
                                                                         end=sample_config().end_date))
    print([d for d in open_iter])
