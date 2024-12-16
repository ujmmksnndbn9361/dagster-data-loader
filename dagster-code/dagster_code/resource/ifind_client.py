from dagster import get_dagster_logger
from iFinDPy import THS_iFinDLogin, THS_HQ, THS_DP
import pandas as pd

logger = get_dagster_logger()


class IFindResource:
    def __init__(self, username, password):
        THS_iFinDLogin(username, password)

    def get_stock_daily(self, code, start_date: pd.Timestamp, end_date: pd.Timestamp):
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        hq_data = THS_HQ(code, 'open,high,low,close,vol,volume,amount,adjustmentFactorBackward1', '',
                         start_date_str,
                         end_date_str)
        if hq_data.errorcode != 0:
            raise RuntimeError(f'获取股票日线数据失败,error msg:{hq_data.errmsg}')
        df: pd.DataFrame = hq_data.data
        columns_mapping = {'adjustmentFactorBackward1': 'adj_factor', 'time': 'trade_date','thscode':'code'}
        df.rename(columns=columns_mapping, inplace=True)
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df['adj_close'] = df['close'] * df['adj_factor']
        # 删除空数据
        df.dropna(subset=['amount'],inplace=True)
        return df
