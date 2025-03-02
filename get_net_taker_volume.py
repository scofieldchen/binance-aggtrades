import datetime as dt
import multiprocessing as mp
import time
from enum import Enum
from pathlib import Path
from typing import Optional

import pandas as pd
import typer
from rich.console import Console

from src.aggtrades_fetcher import MarketType

app = typer.Typer(help="计算加密货币交易的净吃单量", add_completion=False)
console = Console()


class ResampleFrequency(str, Enum):
    """
    数据重采样的频率，成员的值与 pandas.resample 的重采样频率相对应。
    """

    ONE_MINUTE = "1min"  # 1 分钟
    FIVE_MINUTES = "5min"  # 5 分钟
    FIFTEEN_MINUTES = "15min"  # 15 分钟
    THIRTY_MINUTES = "30min"  # 30 分钟
    ONE_HOUR = "1h"  # 1 小时
    FOUR_HOURS = "4h"  # 4 小时
    ONE_DAY = "D"  # 1 天


def read_daily_aggtrades(
    data_dir: str, symbol: str, date: dt.date, market_type: MarketType = MarketType.SPOT
) -> pd.DataFrame:
    """读取指定交易对和日期的聚合交易数据。

    从数据目录中读取特定交易对和日期的所有聚合交易数据文件，
    并将它们合并为一个DataFrame。

    Args:
        data_dir: 数据目录路径
        symbol: 交易对符号，如 "BTCUSDT"
        date: 要读取的日期
        market_type: 市场数据类型，spot, futures

    Returns:
        包含该日期所有聚合交易数据的DataFrame，按时间戳排序

    Raises:
        FileNotFoundError: 当指定日期的数据目录不存在时抛出
        ValueError: 当指定日期没有找到任何数据文件时抛出
    """
    data_path = (
        Path(data_dir)
        / f"{market_type.value}"
        / f"{symbol}"
        / f"{date.year:04d}"
        / f"{date.month:02d}"
        / f"{symbol}_{date:%Y%m%d}.parquet"
    )

    if not data_path.exists():
        raise FileNotFoundError(f"无法找到文件: {data_path}")

    return pd.read_parquet(data_path)


def calculate_net_taker_volume(
    trades: pd.DataFrame, frequency: ResampleFrequency
) -> pd.DataFrame:
    """计算指定时间范围内的净吃单量。

    Args:
        trades: 包含交易数据的DataFrame
        timeframe: 时间范围

    Returns:
        包含净吃单量的DataFrame
    """

    def _resample(df: pd.DataFrame) -> pd.Series:
        """对DataFrame进行重采样并计算净吃单量。"""
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

    res = trades.set_index("timestamp").resample(frequency.value).apply(_resample)

    return res


def _process_single_date(
    date: dt.date,
    data_dir: str,
    symbol: str,
    market_type: MarketType,
    frequency: ResampleFrequency,
) -> Optional[pd.DataFrame]:
    """读取并处理单日数据，计算净吃单量。

    Args:
        date: 要处理的日期
        data_dir: 数据目录路径
        symbol: 交易对符号
        market_type: 市场类型
        frequency: 重采样频率

    Returns:
        处理后的DataFrame，如果处理失败则返回None
    """
    try:
        df = read_daily_aggtrades(data_dir, symbol, date, market_type)
        daily_result = calculate_net_taker_volume(df, frequency)
        return daily_result
    except Exception as e:
        console.print(f"处理 {date} 的数据时发生错误: {e}", style="yellow")
        return None


def process_date_range(
    data_dir: str,
    symbol: str,
    start_date: dt.date,
    end_date: dt.date,
    market_type: MarketType = MarketType.SPOT,
    frequency: ResampleFrequency = ResampleFrequency.ONE_HOUR,
    processes: int = 1,
) -> pd.DataFrame:
    """处理指定日期范围内的交易数据并计算净吃单量。

    Args:
        data_dir: 数据目录路径
        symbol: 交易对符号
        start_date: 开始日期
        end_date: 结束日期
        market_type: 市场类型，默认为现货
        frequency: 重采样频率，默认为1小时
        processes: 用于并行处理的进程数

    Returns:
        包含净吃单量的DataFrame
    """
    # 计算日期范围
    date_range = [
        start_date + dt.timedelta(days=i)
        for i in range((end_date - start_date).days + 1)
    ]

    # 使用多进程并行处理
    if processes > 1:
        with mp.Pool(processes=processes) as pool:
            all_data = pool.starmap(
                _process_single_date,
                [
                    (date, data_dir, symbol, market_type, frequency)
                    for date in date_range
                ],
            )
    else:
        all_data = [
            _process_single_date(date, data_dir, symbol, market_type, frequency)
            for date in date_range
        ]

    # 过滤掉None值
    all_data = [df for df in all_data if df is not None]

    if not all_data:
        raise Exception(f"No data found")

    # 合并所有日期的数据
    result = pd.concat(all_data)

    return result


def generate_output_filename(
    symbol: str,
    market_type: MarketType,
    frequency: ResampleFrequency,
    year: Optional[int] = None,
) -> str:
    """生成输出文件名。

    Args:
        symbol: 交易对名称
        market_type: 市场类型
        frequency: 重采样频率
        year: 年份,如果指定则生成该年份的文件名

    Returns:
        输出文件名
    """
    base_name = f"net_taker_{market_type.value}_{symbol}_{frequency.value}"
    if year is not None:
        return f"{base_name}_{year}.csv"
    return f"{base_name}.csv"


@app.command()
def main(
    data_dir: str = typer.Option(..., help="存储交易数据的文件夹路径"),
    symbol: str = typer.Option(..., help="交易对名称，例如 BTCUSDT"),
    start_date: dt.datetime = typer.Option(
        ...,
        help="开始日期 (YYYY-MM-DD)",
        formats=["%Y-%m-%d"],
    ),
    end_date: dt.datetime = typer.Option(
        ...,
        help="结束日期 (YYYY-MM-DD)",
        formats=["%Y-%m-%d"],
    ),
    market_type: MarketType = typer.Option(
        MarketType.SPOT, help="市场类型，现货或者合约"
    ),
    frequency: ResampleFrequency = typer.Option(
        ResampleFrequency.ONE_HOUR, help="重采样频率"
    ),
    processes: int = typer.Option(1, help="用于并行处理的进程数"),
    group_by_year: bool = typer.Option(False, help="是否按年份分组保存数据"),
) -> None:
    """计算指定日期范围内的净吃单量并输出结果。"""
    start_date = start_date.date()
    end_date = end_date.date()

    console.print("处理数据...")

    t0 = time.time()

    try:
        # 并行处理数据
        result = process_date_range(
            data_dir, symbol, start_date, end_date, market_type, frequency, processes
        )

        # 展示结果
        console.print("\n结果汇总:", style="bold")
        console.print("\n前面5行:", style="bold")
        console.print(result.head())
        console.print("\n后面5行:", style="bold")
        console.print(result.tail())

        # 保存结果
        if group_by_year:
            # 按年份分组保存
            for year, year_data in result.groupby(result.index.year):
                filename = generate_output_filename(
                    symbol, market_type, frequency, year
                )
                year_data.to_csv(filename, index=True)
                console.print(f"\n{year}年数据已保存至: {filename}")
        else:
            # 保存为单个文件
            filename = generate_output_filename(symbol, market_type, frequency)
            result.to_csv(filename, index=True)
            console.print(f"\n结果已保存至: {filename}")

        console.print(f"\n任务完成，耗时 {time.time() - t0:.2f} 秒")

    except Exception as e:
        console.print(f"程序异常: {e}", style="red")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
