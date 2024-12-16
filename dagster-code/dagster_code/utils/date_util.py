import pandas as pd


def split_exec_date_range(start_date, end_date, interval=10):
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    # 将日期范围按interval天一组进行分组
    groups = [date_range[i:i + interval] for i in range(0, len(date_range), interval)]
    result = []
    for group in groups:
        result.append((group[0], group[-1]))
    result.reverse()
    return result