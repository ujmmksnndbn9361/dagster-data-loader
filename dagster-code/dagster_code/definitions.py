from dagster import Definitions, load_assets_from_modules, EnvVar, define_asset_job, ScheduleDefinition, AssetSelection

from dagster_code.assets import tushare_assets, ck_sync_assets
from dagster_code.resource.clickhouse import ClickhouseBareResource
from dagster_code.resource.ts_client import TuShareResource

all_assets = load_assets_from_modules([tushare_assets, ck_sync_assets])
all_assets_job = define_asset_job(name="all_assets_job")

ck_schedule = ScheduleDefinition(name="ck_copyer_data_update",
                                 target=AssetSelection.assets("ck_sync_copy_all_asset"),
                                 cron_schedule="0 * * * *", )

tushare_schedule = ScheduleDefinition(name="tushare_all_data_update",
                                      target=AssetSelection.groups("TuShareSource"),
                                      cron_schedule="0 16,18,20 * * *", )

defs = Definitions(
    assets=all_assets,
    resources={
        'clickhouse': ClickhouseBareResource(
            host=EnvVar('CLICKHOUSE_HOST').get_value(),
            port=EnvVar('CLICKHOUSE_PORT').get_value(),
            user=EnvVar('CLICKHOUSE_USERNAME').get_value(),
            password=EnvVar('CLICKHOUSE_PASSWORD').get_value(),
            database=EnvVar('CLICKHOUSE_DATABASE').get_value(),
        ),
        'tushare': TuShareResource(token=EnvVar('TUSHARE_TOKEN').get_value()),
    },
    jobs=[all_assets_job],
    schedules=[ck_schedule,tushare_schedule]
)
