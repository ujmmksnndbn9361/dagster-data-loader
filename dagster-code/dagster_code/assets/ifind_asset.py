from datetime import datetime
from iFinDPy import THS_iFinDLogin, THS_HQ, THS_DP

import pandas as pd
import numpy as np
from dagster import (
    asset,
    Field,
    MaterializeResult,
    MetadataValue,
    get_dagster_logger,
    ResourceParam,
    AssetExecutionContext
)

from dagster_code.assets.clickhouse_base import ClickhouseBaseLoader, Source, Interval
from dagster_code.resource.clickhouse import ClickhouseBareResource
from dagster_code.resource.ifind_client import IFindResource

# from loguru import logger

DAILY_FORMAT = "%Y-%m-%d"

logger = get_dagster_logger()


class ClickhouseIFindLoader(ClickhouseBaseLoader):

    def __init__(self, start_datetime, end_datetime, clickhouse_resource, ifind_resource: IFindResource):
        self.ifind_resource = ifind_resource
        super().__init__(start_datetime, end_datetime, clickhouse_resource)

    def source(self) -> Source:
        return Source.IFIND_A

    def interval(self) -> Interval:
        return Interval.DAY

    def get_instrument_list(self) -> list:
        stock_df = self.clickhouse_resource.get_tushare_stock()
        return stock_df['ts_code'].to_list()

    def get_execute_date_range(self, start_datetime: pd.Timestamp, end_datetime) -> list:
        return self.clickhouse_resource.get_date_range(start_datetime, end_datetime,
                                                             table_name=self.table_name_raw)

    def get_symbol_data(self, symbol: str, interval: str, start_datetime: pd.Timestamp,
                        end_datetime: pd.Timestamp) -> pd.DataFrame:
        return self.ifind_resource.get_stock_daily(symbol, start_datetime, end_datetime)


@asset(
    group_name='IFindStock',
    config_schema={
        'begin': Field(str, description='开始时间,eg:2024-01-01', is_required=True),
        'end': Field(str, description='结束时间,eg:2024-02-01', is_required=True)
    },
    tags={'env': 'develop'},
)
def ifind_cn_stock_range(context, clickhouse: ResourceParam[ClickhouseBareResource],
                         ifind: ResourceParam[IFindResource]) -> MaterializeResult:
    """
    证券宝日频K线数据下载，下载指定时间范围
    :return:
    """
    config = context.op_config
    ifind_loader: ClickhouseIFindLoader = ClickhouseIFindLoader(start_datetime=config['begin'],
                                                                end_datetime=config['end'],
                                                                clickhouse_resource=clickhouse,
                                                                ifind_resource=ifind)
    ifind_loader.download_data()
    return MaterializeResult(
        metadata={
            'table_name': MetadataValue.text(ifind_loader.table_name_raw)
        }
    )

