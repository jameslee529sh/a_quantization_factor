""" 构建用于因子研究的股票样本数据池
"""
from typing import NamedTuple, Iterator, Text, Tuple, Callable, List, Dict, Optional
from collections import namedtuple
from functools import partial

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


def list_on_updated_date(updated_date: Text,
                         trade_data_func: Callable[[Text], List[Tuple]]) -> Iterator[Text]:
    return trade_data_func(f"select * from daily_trading_data where trade_date='{updated_date}'")


def list_is_trading(updated_it: Iterator[Iterator]) -> Iterator[NamedTuple]:
    return filter(lambda d: d.amount > 1, (d for d in updated_it))


def list_can_be_bought(updated_it: Iterator[Iterator]) -> Iterator[NamedTuple]:
    return filter(lambda d: d.pct_chg < 0.096, (list_it for list_it in updated_it))


def list_is_not_st(updated_it: Iterator[Iterator],
                   name_history_func: Callable[[Text], Iterator[NamedTuple]]) -> Iterator[NamedTuple]:
    # ToDo: tushare
    def name_is_st(ts_code: Text, updated_date: Text) -> bool:
        def date_in_range(r: NamedTuple) -> bool:
            return r.start_date <= updated_date and (r.end_date is None or updated_date <= r.end_date)

        name_history_it: Iterator[NamedTuple] = name_history_func(f"select * from name_history")
        name_on_updated_it: Iterator[NamedTuple] = filter(date_in_range, name_history_it)
        name_is_st_it: Iterator[NamedTuple] = filter(lambda record: record.name.find('ST') >= 0, name_on_updated_it)
        name_is_st_dict: Dict = {r.ts_code: r for r in name_is_st_it}
        return ts_code not in name_is_st_dict

    return filter(lambda r: name_is_st(r.ts_code, r.trade_date), (list_it for list_it in updated_it))


if __name__ == "__main__":
    updated_iter: Iterator[Text] = filter_updated_date(td.imp_get_trade_cal(start=sample_config().start_date,
                                                                            end=sample_config().end_date))
    list_iter: Iterator[Iterator[NamedTuple]] = map(partial(list_on_updated_date,
                                                            trade_data_func=td.imp_get_records_from_db),
                                                    updated_iter)
    list_is_trading_it: Iterator[Iterator[NamedTuple]] = map(list_is_trading, list_iter)
    list_can_be_bought_it: Iterator[Iterator[NamedTuple]] = map(list_can_be_bought, list_is_trading_it)

    list_is_not_st_it: Iterator[Iterator[NamedTuple]] = map(partial(list_is_not_st,
                                                                    name_history_func=td.imp_get_records_from_db),
                                                            list_can_be_bought_it)

    it = (d for updated_iter in list_is_not_st_it for d in updated_iter)
    print(next(it))
    print(next(it))
