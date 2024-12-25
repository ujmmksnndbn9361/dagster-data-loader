import tushare as ts
import pandas as pd
from tushare.pro.client import DataApi
from dagster import get_dagster_logger

logger = get_dagster_logger()


class TuShareResource:
    def __init__(self, token):
        self._ts_client = ts.pro_api(token=token)

    @property
    def ts_client(self) -> DataApi:
        return self._ts_client

    def get_stock_daily(self, start_date, end_date):
        logger.info(f'开始获取股票日线数据，时间范围：{start_date} -> {end_date}')
        fields = ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol",
                  "amount"]
        offset = 0
        limit = 5000
        count = 0
        all_daily_data = []
        # 循环查询日频K线数据
        while True:
            temp_daily_df = self._ts_client.daily(start_date=start_date, end_date=end_date, offset=offset,
                                                  limit=5000,
                                                  fields=fields)
            if not temp_daily_df.empty:
                all_daily_data.append(temp_daily_df)
                count += temp_daily_df.shape[0]
                logger.info(
                    f'fetch stock data {start_date}->{end_date}, offset:{offset}, count:{temp_daily_df.shape[0]} total:{count}')
                offset += limit
            # 如果没有更多数据，退出
            else:
                logger.info(f'{start_date}->{end_date} , offset:{offset} no more data')
                break
        offset = 0
        all_factor_data = []
        count = 0
        while True:
            temp_factor_df = self._ts_client.adj_factor(start_date=start_date, end_date=end_date, offset=offset,
                                                        limit=5000)
            # 更新偏移量
            if not temp_factor_df.empty:
                all_factor_data.append(temp_factor_df)
                count += temp_factor_df.shape[0]
                logger.info(
                    f'fetch factor data {start_date}->{end_date}, offset:{offset}, count:{temp_factor_df.shape[0]} total:{count}')
                offset += limit
            else:
                logger.info(f'{start_date}->{end_date} , offset:{offset} no more data')
                break
        if len(all_daily_data) == 0:
            return None
        # 将所有数据合并为一个DataFrame
        daily_df = pd.concat(all_daily_data, ignore_index=True)
        factor_df = pd.concat(all_factor_data, ignore_index=True)

        final_df = pd.merge(daily_df, factor_df, on=['ts_code', 'trade_date'], how='left')
        final_df['trade_date'] = pd.to_datetime(final_df['trade_date'])
        field_mapping = {'vol': 'volume'}
        final_df.rename(columns=field_mapping, inplace=True)
        final_df.fillna(value=0, inplace=True)

        logger.info(
            f'{start_date} -> {end_date} stock length:{daily_df.shape[0]} '
            f'factor length:{len(factor_df)} final length:{final_df.shape[0]}')
        return final_df

    def get_stock_daily_basic(self, start_date, end_date):
        logger.info(f'开始获取股票日线基本数据，时间范围：{start_date} -> {end_date}')
        fields = [
            "ts_code", "trade_date", "close", "turnover_rate", "turnover_rate_f", "volume_ratio", "pe", "pe_ttm", "pb",
            "ps", "ps_ttm", "dv_ratio", "dv_ttm", "total_share", "float_share", "free_share", "total_mv", "circ_mv", ]

        daily_basic_df = self.fetch_all_data(self._ts_client.daily_basic, start_date=start_date, end_date=end_date,
                                             fields=fields)
        daily_basic_df['trade_date'] = pd.to_datetime(daily_basic_df['trade_date'])
        logger.info(f'{start_date} -> {end_date} daily basic length:{daily_basic_df.shape[0]} ')
        return daily_basic_df

    def get_futures_daily(self, start_date, end_date):
        logger.info(f'开始获取期货日线基本数据，时间范围：{start_date} -> {end_date}')
        fields = ["ts_code", "trade_date", "pre_close", "pre_settle", "open", "high", "low", "close", "settle",
                  "change1", "change2", "vol", "amount", "oi", "oi_chg"]

        final_df = self.fetch_all_data(self._ts_client.fut_daily, start_date=start_date, end_date=end_date,
                                       fields=fields)
        final_df['trade_date'] = pd.to_datetime(final_df['trade_date'])
        field_mapping = {'vol': 'volume'}
        final_df.rename(columns=field_mapping, inplace=True)
        final_df.fillna(value=0, inplace=True)
        logger.info(f'{start_date} -> {end_date} futures daily length:{final_df.shape[0]} ')
        return final_df

    def get_top_list_daily(self, start_date, end_date):
        logger.info(f'开始获取龙虎榜每日明细数据，时间范围：{start_date} -> {end_date}')
        fields = ["trade_date", "ts_code", "name", "close", "pct_change", "turnover_rate", "amount", "l_sell", "l_buy",
                  "l_amount", "net_amount", "net_rate", "amount_rate", "float_values", "reason"]

        final_df = self.fetch_all_data(self._ts_client.top_list, trade_date=start_date, fields=fields)
        if final_df is None:
            return None
        final_df['trade_date'] = pd.to_datetime(final_df['trade_date'])
        final_df.fillna(value=0, inplace=True)
        logger.info(f'{start_date} -> {end_date} top list daily length:{final_df.shape[0]} ')
        return final_df

    def get_top_inst_daily(self, start_date, end_date):
        logger.info(f'开始获取龙虎榜每日明细数据，时间范围：{start_date} -> {end_date}')
        fields = ["trade_date", "ts_code", "exalter", "buy", "buy_rate", "sell", "sell_rate", "net_buy", "side",
                  "reason"]

        final_df = self.fetch_all_data(self._ts_client.top_inst, trade_date=start_date, fields=fields)
        if final_df is None:
            return None
        final_df['trade_date'] = pd.to_datetime(final_df['trade_date'])
        final_df.fillna(value=0, inplace=True)
        logger.info(f'{start_date} -> {end_date} top inst daily length:{final_df.shape[0]} ')
        return final_df

    def fetch_all_data(self, func, date_fields=[], limit=3000, *args, **kwargs):
        offset = 0
        all_data = []
        while True:
            temp_df = func(*args, **kwargs, offset=offset, limit=limit)
            logger.info(
                f'fetch all data func:{func.args[0]} args:{args} kwargs:{kwargs} offset:{offset}, count:{temp_df.shape[0]}')
            if temp_df.empty:
                break
            all_data.append(temp_df)
            offset += limit
        if not all_data:
            return None
        all_df = pd.concat(all_data, ignore_index=True)
        all_df['create_time'] = pd.Timestamp.now()
        for col in date_fields:
            all_df[col] = pd.to_datetime(all_df[col])
        logger.info(f'over! func:{func.args[0]} args:{args} kwargs:{kwargs} final length:{all_df.shape[0]}')
        return all_df
