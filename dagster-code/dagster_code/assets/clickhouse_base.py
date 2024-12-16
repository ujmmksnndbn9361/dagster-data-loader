import abc
import os
from typing import Tuple, Union, Any

import pandas as pd
from enum import Enum
from loguru import logger
from dagster import EnvVar, get_dagster_logger
from pandas import DataFrame, Timestamp
from clickhouse_connect.driver.client import Client
from dagster_code.resource.clickhouse import ClickhouseBareResource


# logger = get_dagster_logger()


class Source(Enum):
    WIND_US = 'wind_us_stock'
    BAO_CN = 'bao_cn_stock'
    IFIND_US = 'ifind_us_stock'
    IFIND_A = 'ifind_a_stock'


class Interval(Enum):
    DAY = 'day'
    MIN_5 = '5min'
    MIN_1 = '1min'


class DataType(Enum):
    # 原始数据
    ORIGINAL = 'o'
    # 基础数据
    FOUNDATION = 'f'


class ClickhouseBaseLoader(abc.ABC):
    def __init__(self,
                 start_datetime,
                 end_datetime,
                 clickhouse_resource,
                 ):
        self.symbols = None
        self.start_datetime = pd.to_datetime(start_datetime)
        self.end_datetime = pd.to_datetime(end_datetime)
        self.table_name_raw = '_'.join(
            [DataType.ORIGINAL.value.lower(), self.source().value.lower(), self.interval().value.lower()])
        self.data_cache: DataFrame = DataFrame()
        self.preview: DataFrame = None
        self.total_count = 0
        self.clickhouse_resource: ClickhouseBareResource = clickhouse_resource
        self.client: Client = self.clickhouse_resource.client

    @abc.abstractmethod
    def source(self) -> Source:
        """
        数据源
        :return:
        """
        raise NotImplementedError("rewrite source")

    @abc.abstractmethod
    def interval(self) -> Interval:
        """
        数据粒度
        :return:
        """
        raise NotImplementedError("rewrite interval")

    @abc.abstractmethod
    def get_instrument_list(self) -> list:
        """
        获取所有symbol
        :return:
        """
        raise NotImplementedError("rewrite get_instrument_list")

    @abc.abstractmethod
    def get_symbol_data(
            self, symbol: str, interval: str, start_datetime: pd.Timestamp, end_datetime: pd.Timestamp
    ) -> pd.DataFrame:
        """
        根据symbol下载数据
        :param symbol: 标识
        :param interval: 间隔
        :param start_datetime:开始时间
        :param end_datetime: 结束时间
        :return: 数据df
        """
        raise NotImplementedError("rewrite get_data")

    def get_calendar_range(self, start_datetime: pd.Timestamp, end_datetime) -> tuple:
        """
        获取执行范围，默认返回原值，可根据不同市场过滤周末等信息
        :return:
        """
        logger.info(f"获取执行日期范围,开始时间:{start_datetime},结束时间:{end_datetime}")
        return start_datetime, end_datetime

    def get_execute_date_range(self, start_datetime: pd.Timestamp, end_datetime) -> list:
        """
        过滤已经执行过的日期
        :return:
        """
        df_result: DataFrame = self.client.query_df(
            f"SELECT max(trade_date) as max, min(trade_date) as min from {self.table_name_raw}")
        if len(df_result) == 0 or df_result['max'][0].strftime("%Y-%m-%d") == '1970-01-01':
            logger.info(f"未获取到历史数据,开始时间:{start_datetime},结束时间:{end_datetime}")
            return [(start_datetime, end_datetime)]
        calendar_min, calendar_max = self.get_calendar_range(start_datetime, end_datetime)

        min_his = pd.Timestamp(df_result['min'].values[0])
        max_his = pd.Timestamp(df_result['max'].values[0])

        start_datetime = max(calendar_min, start_datetime)
        end_datetime = min(calendar_max, end_datetime)

        result = []
        if start_datetime < min_his:
            result.append((start_datetime, min_his - pd.Timedelta(days=1)))
        if end_datetime > max_his:
            result.append((max_his + pd.Timedelta(days=1), end_datetime))
        logger.info(f"获取到历史日期,最终时间范围:{result}")
        return result

    def download_data(self):
        """
        查询instrument_list 并循环下载每个instrument的数据
        :return:
        """
        logger.info(
            f"开始下载数据,数据源:{self.source().value} 数据颗粒度:{self.interval().value} 数据范围:{self.start_datetime}->{self.end_datetime}")
        if not self.symbols:
            self.symbols = self.get_instrument_list()

        symbol_size = len(self.symbols)
        logger.info(f"获取下载Symbol完成,数量:{symbol_size}")
        execute_date_list = self.get_execute_date_range(self.start_datetime, self.end_datetime)
        if len(execute_date_list) == 0:
            logger.info(f"{self.start_datetime}->{self.end_datetime} 数据已存在或不在交易日,没有需要下载的数据")
            return
        for i, symbol in enumerate(self.symbols):
            for date_range in execute_date_list:
                df = self.get_symbol_data(symbol, self.interval().value, date_range[0], date_range[1])
                if df.empty:
                    logger.warning(f"{symbol} 数据为空")
                    continue
                logger.info(f"{symbol} 下载完成,数据量:{len(df)},当前进度:{i + 1}/{symbol_size}")
                if df is None or len(df) > 0:
                    self.preview = df
                self.batch_insert_data(df)
        if self.data_cache is not None and not self.data_cache.empty:
            self.batch_insert_data(force=True)
        logger.info(f"数据下载完成,总数据量:{self.total_count}")

    def batch_insert_data(self, df: DataFrame = DataFrame(), count=500, force=False):
        """
        写入数据，数据量大于count条时进行写入，避免频繁写入
        """
        if not force:
            self.data_cache = pd.concat([self.data_cache, df], ignore_index=True)
        if len(self.data_cache) > count or (force and len(self.data_cache) > 0):
            try:
                self.client.insert_df(self.table_name_raw, self.data_cache)
            except Exception as e:
                logger.error(f'{self.data_cache}')
                logger.error(f'数据写入失败，原因:{e}')
                raise e
            logger.info(f'数据写入成功，写入数据量:{len(self.data_cache)}')
            self.total_count += len(self.data_cache)
            self.data_cache = DataFrame()
