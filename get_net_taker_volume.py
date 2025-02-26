import datetime as dt
from pathlib import Path
from enum import Enum

import pandas as pd


class TimeFrame(Enum):
    """
    K线时间框架枚举类，成员的值与 pandas.resample 的重采样频率相对应。
    """

    ONE_MINUTE = "1T"  # 1 分钟
    FIVE_MINUTES = "5T"  # 5 分钟
    FIFTEEN_MINUTES = "15T"  # 15 分钟
    THIRTY_MINUTES = "30T"  # 30 分钟
    ONE_HOUR = "1H"  # 1 小时
    FOUR_HOURS = "4H"  # 4 小时
    ONE_DAY = "1D"  # 1 天


def read_daily_aggtrades(data_dir: str, symbol: str, date: dt.date) -> pd.DataFrame:
    data_path_by_symbol_date = (
        Path(data_dir)
        / f"symbol={symbol}"
        / f"year={date.year:04d}"
        / f"month={date.month:02d}"
        / f"day={date.day:02d}"
    )

    files = [item for item in data_path_by_symbol_date.glob("*.parquet")]
    df = pd.concat((pd.read_parquet(file) for file in files))

    df.set_index("timestamp", inplace=True)
    df.sort_index(inplace=True)

    return df


def calculate_net_taker_volume(
    trades: pd.DataFrame, timeframe: TimeFrame
) -> pd.DataFrame:

    def _resample(df: pd.DataFrame) -> pd.Series:
        # 买入吃单，is_buyer_maker=false，卖方使用限价单，买方使用市价单主动买入
        taker_buy_volume = df.query("is_buyer_maker == False")["quantity"].sum()

        # 卖出吃单，is_buyer_maker=true，买方使用限价单，卖方使用市价单主动卖出
        taker_sell_volume = df.query("is_buyer_maker == True")["quantity"].sum()

        # 净吃单量，单位是计价货币
        net_taker_volume = taker_buy_volume - taker_sell_volume

        return pd.Series(
            {
                "open": df["price"].iloc[0],
                "high": df["price"].max(),
                "low": df["price"].min(),
                "close": df["price"].iloc[-1],
                "volume": df["quantity"].sum(),
                "net_taker_volume": net_taker_volume,
            }
        )

    res = trades.resample(timeframe.value).apply(_resample)

    return res


data_dir = "/Users/scofield/aggtrades/data"
symbol = "BTCUSDT"
date = dt.date(2025, 2, 1)

df = read_daily_aggtrades(data_dir, symbol, date)
# print(df.head())
# print(df.tail())
# print(df.info())

net_taker = calculate_net_taker_volume(df, TimeFrame.ONE_HOUR)
print(net_taker.head())
print(net_taker.tail())
