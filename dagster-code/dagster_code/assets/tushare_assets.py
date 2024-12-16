import pandas as pd
from dagster import (
    asset,
    Field,
    MaterializeResult,
    MetadataValue,
    get_dagster_logger,
    ResourceParam,
    AssetExecutionContext,
)

from dagster_code.resource.clickhouse import ClickhouseBareResource
from dagster_code.resource.ts_client import TuShareResource
from datetime import datetime

logger = get_dagster_logger()


@asset(
    group_name='TuShareSource',
    tags={'env': 'production'},
)
def tushare_trade_calendar(context, clickhouse: ResourceParam[ClickhouseBareResource],
                           tushare: ResourceParam[TuShareResource]) -> MaterializeResult:
    """
    TuShare交易日
    """
    table_name = 'quant_data.o_tushare_a_stock_trade_calendar'
    last_time = clickhouse.max_time_value(table_name, column_name='cal_date')
    logger.info(f'交易日历数据最大日期：{last_time}')
    count = 0

    if last_time is None or last_time < pd.Timestamp.now():
        cal = tushare.ts_client.trade_cal(exchange="SSE", start_date=19491001, end_date=20991231,
                                          fields=["cal_date", "is_open"])
        cal['cal_date'] = pd.to_datetime(cal['cal_date'])
        clickhouse.client.insert_df(table=table_name, df=cal)
        count = cal.shape[0]
        clickhouse.optimize_table(table_name)
        logger.info(f'下载交易日历数据：{count}条')
    else:
        logger.info(f'交易日历数据已存在，跳过下载')
    return MaterializeResult(
        metadata={
            'table_name': MetadataValue.text(table_name)
        }
    )


@asset(
    group_name='TuShareSource',
    tags={'env': 'production'},
)
def tushare_a_trade_info(context, clickhouse: ResourceParam[ClickhouseBareResource],
                         tushare: ResourceParam[TuShareResource]) -> MaterializeResult:
    """
    TuShare股票基本信息
    """
    table_name = 'quant_data.o_tushare_a_stock_info'
    last_time = clickhouse.max_time_value(table_name)
    logger.info(f'股票列表最新更新时间：{last_time}')

    if last_time is None or last_time < pd.Timestamp.now().replace(hour=0, minute=0, second=0, microsecond=0):
        basic_df = tushare.ts_client.stock_basic(
            fields=["ts_code", "name", "fullname", "cnspell", "area", "industry", "market", "exchange", "list_status",
                    "list_date", "delist_date"])
        basic_df['list_date'] = pd.to_datetime(basic_df['list_date'])
        basic_df['delist_date'] = pd.to_datetime(basic_df['delist_date'])
        basic_df['create_time'] = datetime.now()
        clickhouse.client.insert_df(table=table_name, df=basic_df)
        count = basic_df.shape[0]
        if last_time is not None:
            clickhouse.optimize_table(table_name)
            logger.info(f'更新数据，优化重复数据')
        logger.info(f'下载股票列表数据：{count}条')
    else:
        logger.info(f'今日股票列表已下载，跳过下载')
    return MaterializeResult(
        metadata={
            'table_name': MetadataValue.text(table_name)
        }
    )


@asset(
    deps=['tushare_trade_calendar'],
    config_schema={
        'start_date': Field(str, description='开始日期，格式：yyyymmdd',
                            default_value=pd.Timestamp.now().strftime('%Y%m%d')),
        'end_date': Field(str, description='结束日期，格式：yyyymmdd',
                          default_value=pd.Timestamp.now().strftime('%Y%m%d')),
    },
    group_name='TuShareSource',
    tags={'env': 'production'},
)
def tushare_a_trade_daily(context, clickhouse: ResourceParam[ClickhouseBareResource],
                          tushare: ResourceParam[TuShareResource]) -> MaterializeResult:
    """
    TuShare日频A股K线数据
    """
    table_name = 'quant_data.o_tushare_a_stock_daily'
    count = 0
    start_date_str, end_date_str = context.op_config['start_date'], context.op_config['end_date']
    logger.info(f'日频交易日期参数：{start_date_str} -> {end_date_str}')

    start_date = pd.to_datetime(start_date_str)
    end_date = pd.to_datetime(end_date_str)

    execute_date_list = clickhouse.get_split_date_range(start_date, end_date, table_name=table_name)
    for date_range in execute_date_list:
        daily_df = tushare.get_stock_daily(start_date=date_range[0].strftime('%Y%m%d'),
                                           end_date=date_range[1].strftime('%Y%m%d'))
        if daily_df is None or daily_df.empty:
            continue
        count += daily_df.shape[0]
        logger.info(f'下载股票日频数据：{date_range[0]} -> {date_range[1]} 数量:{daily_df.shape[0]}')
        clickhouse.client.insert_df(table=table_name, df=daily_df)
    return MaterializeResult(
        metadata={
            'table_name': MetadataValue.text(table_name)
        }
    )


@asset(
    deps=['tushare_trade_calendar'],
    config_schema={
        'start_date': Field(str, description='开始日期，格式：yyyymmdd',
                            default_value=pd.Timestamp.now().strftime('%Y%m%d')),
        'end_date': Field(str, description='结束日期，格式：yyyymmdd',
                          default_value=pd.Timestamp.now().strftime('%Y%m%d')),
    },
    tags={'env': 'production'},
    group_name='TuShareSource',
)
def tushare_a_trade_daily_basic(context, clickhouse: ResourceParam[ClickhouseBareResource],
                                tushare: ResourceParam[TuShareResource]) -> MaterializeResult:
    """
    TuShare日频指标数据
    """
    table_name = 'quant_data.o_tushare_a_stock_daily_basic'
    count = 0
    start_date_str, end_date_str = context.op_config['start_date'], context.op_config['end_date']
    logger.info(f'日频指标数据 日期参数：{start_date_str} -> {end_date_str}')

    start_date = pd.to_datetime(start_date_str)
    end_date = pd.to_datetime(end_date_str)

    execute_date_list = clickhouse.get_split_date_range(start_date, end_date, table_name=table_name)
    for date_range in execute_date_list:
        daily_df = tushare.get_stock_daily_basic(start_date=date_range[0].strftime('%Y%m%d'),
                                                 end_date=date_range[1].strftime('%Y%m%d'))
        count += daily_df.shape[0]
        logger.info(f'下载日频指标数据：{date_range[0]} -> {date_range[1]} 数量:{daily_df.shape[0]}')
        clickhouse.client.insert_df(table=table_name, df=daily_df)
    return MaterializeResult(
        metadata={
            'table_name': MetadataValue.text(table_name)
        }
    )


@asset(
    config_schema={
        'refresh': Field(bool, description='是否刷新已有数据', default_value=False)
    },
    group_name='TuShareSource',
    tags={'env': 'production'},

)
def tushare_a_trade_sw_industry_classify_2021(context, clickhouse: ResourceParam[ClickhouseBareResource],
                                              tushare: ResourceParam[TuShareResource]) -> MaterializeResult:
    """
    TuShare申万行业分类
    """
    table_name = 'quant_data.o_tushare_sw_industry_classify_2021'
    refresh_ = context.op_config['refresh']
    if refresh_:
        clickhouse.truncate_table(table_name)
        logger.info(f'强制刷新表数据：{table_name}')
    last_time = clickhouse.max_time_value(table_name)
    if last_time is None:
        all_df = tushare.fetch_all_data(func=tushare.ts_client.index_classify, src='SW2021')
        clickhouse.client.insert_df(table=table_name, df=all_df)
        logger.info(f'下载申万行业分类数据完成：{all_df.shape[0]}条')
    else:
        logger.info(f'申万行业分类数据已下载，跳过下载')
    return MaterializeResult(
        metadata={
            'table_name': MetadataValue.text(table_name)
        }
    )


@asset(
    deps=['tushare_trade_calendar'],
    config_schema={
        'start_date': Field(str, description='开始日期，格式：yyyymmdd',
                            default_value=pd.Timestamp.now().strftime('%Y%m%d')),
        'end_date': Field(str, description='结束日期，格式：yyyymmdd',
                          default_value=pd.Timestamp.now().strftime('%Y%m%d')),
    },
    group_name='TuShareSource',
    tags={'env': 'production'},
)
def tushare_futures_daily(context, clickhouse: ResourceParam[ClickhouseBareResource],
                          tushare: ResourceParam[TuShareResource]) -> MaterializeResult:
    """
    TuShare日频期货数据
    """
    table_name = 'quant_data.o_tushare_futures_daily'
    count = 0
    start_date_str, end_date_str = context.op_config['start_date'], context.op_config['end_date']
    logger.info(f'日频期货交易日期参数：{start_date_str} -> {end_date_str}')

    start_date = pd.to_datetime(start_date_str)
    end_date = pd.to_datetime(end_date_str)

    execute_date_list = clickhouse.get_split_date_range(start_date, end_date, table_name=table_name)
    for date_range in execute_date_list:
        daily_df = tushare.get_futures_daily(start_date=date_range[0].strftime('%Y%m%d'),
                                           end_date=date_range[1].strftime('%Y%m%d'))
        if daily_df is None or daily_df.empty:
            continue
        count += daily_df.shape[0]
        logger.info(f'下载期货日频数据：{date_range[0]} -> {date_range[1]} 数量:{daily_df.shape[0]}')
        clickhouse.client.insert_df(table=table_name, df=daily_df)
    return MaterializeResult(
        metadata={
            'table_name': MetadataValue.text(table_name)
        }
    )

@asset(
    config_schema={
        'refresh': Field(bool, description='是否刷新已有数据', default_value=False)
    },
    group_name='TuShareSource',
    tags={'env': 'production'},
)
def tushare_a_trade_sw_industry_member(context, clickhouse: ResourceParam[ClickhouseBareResource],
                                       tushare: ResourceParam[TuShareResource]) -> MaterializeResult:
    """
    TuShare申万行业分类成分
    """
    table_name = 'quant_data.o_tushare_sw_industry_member'
    refresh_ = context.op_config['refresh']
    if refresh_:
        clickhouse.truncate_table(table_name)
        logger.info(f'强制刷新表数据：{table_name}')
    last_time = clickhouse.max_time_value(table_name)
    if last_time is None:
        all_df = tushare.fetch_all_data(func=tushare.ts_client.index_member_all, date_fields=['in_date', 'out_date'])
        clickhouse.client.insert_df(table=table_name, df=all_df)
        logger.info(f'下载申万行业成分数据完成：{all_df.shape[0]}条')
    else:
        logger.info(f'申万行业成分数据已下载，跳过下载')
    return MaterializeResult(
        metadata={
            'table_name': MetadataValue.text(table_name)
        }
    )
