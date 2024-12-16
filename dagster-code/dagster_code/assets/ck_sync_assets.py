from datetime import datetime
from clickhouse_connect.driver.client import Client
from pandas import DataFrame
from loguru import logger
from dagster import (
    asset,
    Field,
    MaterializeResult,
    MetadataValue,
    get_dagster_logger,
    ResourceParam,
)

from dagster_code.resource.clickhouse import ClickhouseBareResource

logger = get_dagster_logger()


class Copyer:
    def __init__(self, index_field, source_table, target_table=None, source_db='firstrate', target_db=None):
        self.index_field = index_field
        self.source_db = source_db
        self.target_db = target_db
        self.source_table = source_table
        if target_table is None:
            self.target_table = source_table
        else:
            self.target_table = target_table
        if target_db is None:
            self.target_db = source_db
        else:
            self.target_db = target_db
        self.copy_name = f'{self.source_db}.{self.source_table}->{self.target_db}.{self.target_table}'


class CopyerManager:
    def __init__(self, copyer_list, target_client, source_host='15.204.140.207', source_port=9000,
                 source_user='usdata1', source_password='usdata;*P.2', ):
        self.ck_client: Client = target_client
        self.copyer_list = copyer_list
        self.source_host = source_host
        self.source_port = source_port
        self.source_user = source_user
        self.source_password = source_password

    def generate_copy_sql(self, copyer: Copyer, trade_date: str = datetime.now().strftime("%Y-%m-%d")):
        result_sql = f"""insert into {copyer.target_db}.{copyer.target_table} 
        select * from {self.remote_str(copyer)}
        """
        if copyer.index_field:
            result_sql += f" where {copyer.index_field} = '{trade_date}'"
        return result_sql

    def truncate_table(self, table_name):
        self.ck_client.command(f'truncate table {table_name}')

    def diff_count(self, copyer: Copyer):
        target_df: DataFrame = self.ck_client.query_df(
            f"SELECT COUNT() AS num FROM {copyer.source_db}.{copyer.source_table}")
        source_df: DataFrame = self.ck_client.query_df(
            f"SELECT COUNT() AS num FROM {self.remote_str(copyer)}")
        return source_df['num'].values[0] - target_df['num'].values[0]

    def diff_index(self, copyer: Copyer) -> set:
        if copyer.index_field is None:
            return {'all'}
        target_df: DataFrame = self.ck_client.query_df(
            f"SELECT DISTINCT {copyer.index_field} FROM {copyer.source_db}.{copyer.source_table}")
        source_df: DataFrame = self.ck_client.query_df(
            f"SELECT DISTINCT {copyer.index_field} FROM {self.remote_str(copyer)}")
        if len(target_df) == 0:
            source_set = {}
        else:
            source_set = set(target_df[copyer.index_field].tolist())
        return set(source_df[copyer.index_field].tolist()).difference(source_set)

    def remote_str(self, copyer: Copyer):
        return f"remote('{self.source_host}:{self.source_port}', '{copyer.source_db}.{copyer.source_table}', '{self.source_user}', '{self.source_password}')"

    def copy_data(self, copyer: Copyer):
        if self.diff_count(copyer) == 0:
            logger.info(f"{copyer.source_table}->{copyer.target_table} 没有数据需要更新")
            return
        if copyer.index_field is None:
            logger.info(f"{copyer.source_table}->{copyer.target_table} 全量更新")
            self.truncate_table(f"{copyer.target_db}.{copyer.target_table}")
        diff_index = sorted(self.diff_index(copyer))
        logger.info(f"{copyer.source_table}->{copyer.target_table} 待更新日期:{diff_index}")
        if len(diff_index) == 0:
            logger.info(f"{copyer.source_table}->{copyer.target_table} 没有数据需要更新")
            return
        for trade_date in diff_index:
            _sql = self.generate_copy_sql(copyer, trade_date)
            self.ck_client.command(_sql)

    def copy_all(self):
        logger.info(f"开始更新数据,数据表数量:{len(self.copyer_list)}")
        for copyer in self.copyer_list:
            logger.info(f"开始更新数据:{copyer.copy_name}")
            self.copy_data(copyer)
            logger.info(f"更新数据完成:{copyer.copy_name}")


@asset(
    group_name='ClickhouseSync',
    tags={'env': 'production'},
)
def ck_sync_copy_all_asset(clickhouse: ResourceParam[ClickhouseBareResource]) -> None:
    """
    远程CK数据库全量&增量同步
    """
    copy_list = [
        Copyer(index_field=None, source_table='company_profiles', source_db='firstrate'),
        Copyer(index_field='toString(TradeDate)', source_table='splits_dividends', source_db='firstrate'),
        Copyer(index_field='toString(TradeDate)', source_table='kline_1d_unajusted', source_db='firstrate'),
        Copyer(index_field='toString(TradeDate)', source_table='kline_1d_splitdiv', source_db='firstrate'),
    ]
    manager = CopyerManager(copy_list, clickhouse.client)
    manager.copy_all()
