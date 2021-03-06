""" 构建用于因子研究的股票样本数据池
"""
from typing import NamedTuple, Iterator, Text, Tuple, Callable, List, Dict, Optional, Set
from collections import namedtuple
from functools import partial, lru_cache, reduce
import datetime

import pandas as pd
import tushare as ts

from src import tushare_data as td
from src import config

Samples = Iterator[NamedTuple]  # 某一日的样本股集合
Sample_pool = Iterator[Samples]  # 样本池

Sample_config: NamedTuple = namedtuple('sample_config', 'start_date, end_date, updated_date1, updated_date2, \
                                        base_index, low_market_to_base_index')
Samples_info: NamedTuple = namedtuple('sample_info', 'date, mean_mv, median_mv, min_mv, count')


def sample_config() -> Sample_config:
    return Sample_config(start_date='20050430', end_date='20190430', updated_date1='0430', updated_date2='1031',
                         base_index='399300.SZ', low_market_to_base_index=0.05)


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


def impf_get_tradable_securities(trade_date: Text) -> Samples:
    return td.imp_get_records_from_db(f"SELECT * FROM daily_trading_data WHERE trade_date='{trade_date}'")


def securities_can_be_bought(sample_it: Samples) -> Samples:
    return filter(lambda d: d.pct_chg < 0.096, sample_it)


def list_is_not_st(updated_it: Samples,
                   name_history_func: Callable[[Text], Iterator[NamedTuple]]) -> Iterator[NamedTuple]:
    def name_is_st(ts_code: Text, updated_date: Text) -> bool:
        def date_in_range(r: NamedTuple) -> bool:
            return r.start_date <= updated_date and (r.end_date is None or updated_date <= r.end_date)

        name_history_it: Iterator[NamedTuple] = name_history_func(f"select * from name_history")
        name_on_updated_it: Iterator[NamedTuple] = filter(date_in_range, name_history_it)
        name_is_st_it: Iterator[NamedTuple] = filter(lambda record: record.name.find('ST') >= 0, name_on_updated_it)
        name_is_st_dict: Dict = {r.ts_code: r for r in name_is_st_it}
        return ts_code not in name_is_st_dict

    return filter(lambda r: name_is_st(r.ts_code, r.trade_date), (list_it for list_it in updated_it))


def list_is_over_years(updated_it: Samples, name_history_func: Callable[[Text], Iterator[NamedTuple]]) -> Samples:
    def list_date(ts_code: Text) -> Text:
        list_date_iter: Iterator[NamedTuple] = name_history_func(f"SELECT MIN(start_date) AS list_date FROM \
                                                                    name_history WHERE ts_code='000002.SZ'")
        return next(list_date_iter).list_date

    def is_over_years(ts_code: Text, trade_date: Text) -> bool:
        delta: datetime.timedelta = datetime.datetime.strptime(trade_date, "%Y%m%d") \
                                    - datetime.datetime.strptime(list_date(ts_code), "%Y%m%d")
        return delta.days > 365 * 2

    return filter(lambda r: is_over_years(r.ts_code, r.trade_date), (list_it for list_it in updated_it))


def market_value_exceeds_low_limit(updated_it: Samples,
                                   index_daily_basic_func: Callable[[Text, Text], NamedTuple],
                                   stock_daily_basic_func: Callable[[Text], Iterator[NamedTuple]]) -> Samples:
    @lru_cache(maxsize=128)
    def index_market_value(trade_date: Text) -> float:
        return index_daily_basic_func(sample_config().base_index, trade_date).total_mv

    def stock_market_value(ts_code: Text, trade_date: Text) -> float:
        daily_basic_iter: Iterator = stock_daily_basic_func(f"SELECT * FROM daily_basic WHERE ts_code='{ts_code}' \
                                                                and trade_date='{trade_date}'")
        return next(daily_basic_iter).total_mv

    return filter(lambda r: index_market_value(r.trade_date) * sample_config().low_market_to_base_index <
                            stock_market_value(r.ts_code, r.trade_date),
                  (list_it for list_it in updated_it))


def impf_get_tradable_securities_by_tushare(trade_date: Text) -> pd.Series:
    df: pd.DataFrame = ts.pro_api(config.tushare_token).daily(trade_date=trade_date)
    return df[df['pct_chg'] < 9.6]


def impf_get_non_st_securities_by_tushare_cache(trade_date: Text) -> pd.DataFrame:
    df: pd.DataFrame = td.imp_get_records_from_db(f"select * from name_history where start_date <= '{trade_date}' \
                                                    and (end_date is null or '{trade_date}' <= end_date)")
    df2 = df[df['name'].apply(lambda c: c.find('ST') < 0)]
    return df2


def impf_get_companies_listed_for_many_years_by_tushare(trade_date: Text) -> pd.DataFrame:
    def is_over_years(listed_date: Text) -> bool:
        delta: datetime.timedelta = datetime.datetime.strptime(trade_date, "%Y%m%d") \
                                    - datetime.datetime.strptime(listed_date, "%Y%m%d")
        return delta.days > 365 * 2

    def is_still_list(delist_date: Text) -> bool:
        return delist_date is None or trade_date < delist_date

    df: pd.DataFrame = td.download_list_companies()
    df2 = df[df['list_date'].map(is_over_years)]
    df3 = df2[df2['delist_date'].map(is_still_list)]
    return df3


def impf_exclude_small_market_value_companies_by_tushare_cache(trade_date: Text) -> pd.DataFrame:
    # index_market_value: float = td.imp_get_records_from_db(f"SELECT total_mv FROM securities_index \
    #                                                         WHERE ts_code='399300.SZ' and trade_date='{trade_date}'")
    index_market_value: float = ts.pro_api(config.tushare_token).index_dailybasic(trade_date=trade_date,
                                                                                  ts_code='399300.SZ').iloc[0][
        'total_mv']
    df: pd.DataFrame = td.imp_get_records_from_db(f"SELECT * FROM daily_basic WHERE trade_date='{trade_date}'")
    low_limit: float = index_market_value / (300 * 50 * 10000)
    return df[df['total_mv'] >= low_limit]


def build_samples(trade_date: Text,
                  get_non_st_securities: Callable[[Text], pd.DataFrame],
                  get_companies_listed_for_many_years: Callable[[Text], pd.DataFrame],
                  get_tradable_securities: Callable[[Text], pd.DataFrame],
                  exclude_small_market_value_companies: Callable[[Text], pd.DataFrame]) -> Tuple[Samples_info, Set]:
    non_st_securities: pd.DataFrame = get_non_st_securities(trade_date)
    companies_listed_for_many_years: pd.DataFrame = get_companies_listed_for_many_years(trade_date)
    tradable_securities: pd.DataFrame = get_tradable_securities(trade_date)
    normal_market_value_companies: pd.DataFrame = exclude_small_market_value_companies(trade_date)
    samples: set = set(non_st_securities['ts_code']).intersection(set(companies_listed_for_many_years['ts_code']))
    samples = samples.intersection(set(tradable_securities['ts_code']))
    samples = samples.intersection(set(normal_market_value_companies['ts_code']))

    market_value_samples: pd.DataFrame = normal_market_value_companies[normal_market_value_companies['ts_code']
        .map(lambda c: c in samples)]
    mv: pd.Series = market_value_samples['total_mv'].describe()
    return Samples_info(date=trade_date, mean_mv=mv['mean'], median_mv=mv['50%'], min_mv=mv['min'], count=mv['count']), \
           samples


impf_build_samples_by_tushare = partial(build_samples,
                                        get_non_st_securities=impf_get_non_st_securities_by_tushare_cache,
                                        get_companies_listed_for_many_years=
                                        impf_get_companies_listed_for_many_years_by_tushare,
                                        get_tradable_securities=impf_get_tradable_securities_by_tushare,
                                        exclude_small_market_value_companies=
                                        impf_exclude_small_market_value_companies_by_tushare_cache)


if __name__ == "__main__":
    # 获取构建样本的时间序列（每年4月30日，10月31日或其后的第一个交易日）
    updated_date_iter: Iterator[Text] = filter_updated_date(td.imp_get_trade_cal(start=sample_config().start_date,
                                                                                 end=sample_config().end_date))

    for updated_date in updated_date_iter:
        samples_info, data = impf_build_samples_by_tushare(updated_date)
        print(samples_info)
