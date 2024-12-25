from typing import Any
import pandas as pd
import clickhouse_connect
from pandas import Timestamp
from dagster import get_dagster_logger
from dagster_code.utils.date_util import split_exec_date_range

logger = get_dagster_logger()


class ClickhouseBareResource:
    def __init__(self, host, port, user, password, database):
        self._client = clickhouse_connect.get_client(host=host, port=port, username=user, password=password,
                                                     database=database)

    @property
    def client(self):
        return self._client

    def max_time_value(self, table_name: str, column_name: str = 'create_time') -> Any | None:
        """
        获取表最后插入时间
        :param table_name: 表名
        :param column_name: 字段名，默认create_time
        :return: 最近更新时间
        """
        query = f'SELECT max({column_name}) AS max FROM {table_name}'
        df = self._client.query_df(query)
        max_ = df.iloc[0]['max']
        return None if max_ == pd.Timestamp('1970-01-01') else max_

    def truncate_table(self, table_name: str):
        self._client.command(f'TRUNCATE TABLE {table_name}')

    def optimize_table(self, table_name: str):
        self._client.command(f'OPTIMIZE TABLE {table_name} FINAL')

    def get_tushare_stock(self):
        return self._client.query_df("SELECT ts_code FROM quant_data.o_tushare_a_stock_info WHERE list_status = 'L'")

    def get_date_range(self, start_date: Timestamp, end_date: Timestamp, table_name: str) -> list:
        """
        获取实际数据下载时间范围
        :param start_date: 开始时间
        :param end_date: 结束时间
        :param table_name:数据表
        :return: [(start_date, end_date),(start_date, end_date)]
        """
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')

        cal_df = self._client.query_df(f"""SELECT max(cal_date) AS max, min(cal_date) AS min
            FROM quant_data.o_tushare_a_stock_trade_calendar
            WHERE cal_date BETWEEN '{start_str}' AND '{end_str}'
              AND is_open = 1""")

        max_cal = cal_df.iloc[0]['max']
        min_cal = cal_df.iloc[0]['min']
        if max_cal == pd.Timestamp('1970-01-01'):
            logger.warning(f'{start_date}->{end_date} 不在交易日')
            return []

        his_df = self._client.query_df(
            f'SELECT MAX(trade_date) AS max, MIN(trade_date) AS min FROM {table_name}')
        max_his = his_df.iloc[0]['max']
        min_his = his_df.iloc[0]['min']

        start_date = max(min_cal, start_date)
        end_date = min(max_cal, end_date)

        if max_his == pd.Timestamp('1970-01-01'):
            logger.error(f'{table_name} 没有历史数据，使用传入日历 {start_date}->{end_date}')
            return [(start_date, end_date)]

        result = []
        if start_date < min_his:
            result.append((start_date, min_his - pd.Timedelta(seconds=24 * 60 * 60)))
        if end_date > max_his:
            result.append((max_his + pd.Timedelta(seconds=24 * 60 * 60), end_date))
        logger.info(f"获取到历史日期,最终执行时间范围:{result}")
        return result

    def get_split_date_range(self, start_date: Timestamp, end_date: Timestamp, table_name: str, interval=10) -> list:
        """
        获取拆分后的实际数据下载时间范围
        :param start_date: 开始时间
        :param end_date: 结束时间
        :param table_name:数据表
        :return: [(start_date, end_date),(start_date, end_date)]
        """
        result = self.get_date_range(start_date, end_date, table_name)
        logger.info(f"获取到历史日期,最终执行时间范围:{result}")
        split_result = [group for x in result for group in split_exec_date_range(x[0], x[1], interval)]
        logger.info(f"拆分后执行时间范围:{split_result}")
        return split_result
