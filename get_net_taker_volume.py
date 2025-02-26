import time
import datetime as dt
from pathlib import Path
from enum import Enum
from typing import Optional, List, Callable

import pandas as pd
import typer
from rich.console import Console
import multiprocessing as mp

console = Console()


class TimeFrame(str, Enum):
    """
    K线时间框架枚举类，成员的值与 pandas.resample 的重采样频率相对应。
    """

    ONE_MINUTE = "1T"  # 1 分钟
    FIVE_MINUTES = "5T"  # 5 分钟
    FIFTEEN_MINUTES = "15T"  # 15 分钟
    THIRTY_MINUTES = "30T"  # 30 分钟
    ONE_HOUR = "1h"  # 1 小时
    FOUR_HOURS = "4h"  # 4 小时
    ONE_DAY = "1D"  # 1 天


def read_daily_aggtrades(data_dir: str, symbol: str, date: dt.date) -> pd.DataFrame:
    """读取指定交易对和日期的聚合交易数据。

    从数据目录中读取特定交易对和日期的所有聚合交易数据文件，
    并将它们合并为一个DataFrame。

    Args:
        data_dir: 数据目录路径
        symbol: 交易对符号，如 "BTCUSDT"
        date: 要读取的日期

    Returns:
        包含该日期所有聚合交易数据的DataFrame，按时间戳排序

    Raises:
        FileNotFoundError: 当指定日期的数据目录不存在时抛出
        ValueError: 当指定日期没有找到任何数据文件时抛出
    """
    data_path_by_symbol_date = (
        Path(data_dir)
        / f"symbol={symbol}"
        / f"year={date.year:04d}"
        / f"month={date.month:02d}"
        / f"day={date.day:02d}"
    )

    # 检查目录是否存在
    if not data_path_by_symbol_date.exists():
        raise FileNotFoundError(f"Data directory not found: {data_path_by_symbol_date}")

    # 获取所有parquet文件
    files = list(data_path_by_symbol_date.glob("*.parquet"))

    # 检查是否有数据文件
    if not files:
        raise ValueError(f"No parquet files found in {data_path_by_symbol_date}")

    # 读取并合并所有文件
    df = pd.concat((pd.read_parquet(file) for file in files))

    df.set_index("timestamp", inplace=True)
    df.sort_index(inplace=True)

    return df


def calculate_net_taker_volume(
    trades: pd.DataFrame, timeframe: TimeFrame
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

    res = trades.resample(timeframe.value).apply(_resample)

    return res


def process_date_range(
    data_dir: str,
    symbol: str,
    start_date: dt.date,
    end_date: dt.date,
    timeframe: TimeFrame = TimeFrame.ONE_HOUR,
    processes: int = 1,
) -> pd.DataFrame:
    """处理指定日期范围内的交易数据并计算净吃单量。

    Args:
        data_dir: 数据目录路径
        symbol: 交易对符号
        start_date: 开始日期
        end_date: 结束日期
        timeframe: 时间范围，默认为1小时
        processes: 用于并行处理的进程数

    Returns:
        包含净吃单量的DataFrame
    """
    # 计算日期范围
    date_range = [
        start_date + dt.timedelta(days=i)
        for i in range((end_date - start_date).days + 1)
    ]

    def _process_date(date: dt.date) -> Optional[pd.DataFrame]:
        """读取、处理单日数据并计算净吃单量。"""
        try:
            df = read_daily_aggtrades(data_dir, symbol, date)
            daily_result = calculate_net_taker_volume(df, timeframe)
            return daily_result
        except Exception as e:
            console.print(f"[yellow]Warning: Could not read data for {date}: {e}[/]")
            return None

    # 使用多进程并行处理
    if processes > 1:
        with mp.Pool(processes=processes) as pool:
            all_data = pool.map(_process_date, date_range)
    else:
        all_data = [_process_date(date) for date in date_range]

    # 过滤掉None值
    all_data = [df for df in all_data if df is not None]

    if not all_data:
        console.print("[red]Error: No data found[/]")
        raise typer.Exit(code=1)

    # 合并所有日期的数据
    result = pd.concat(all_data)
    # result.sort_index(inplace=True) # 不需要排序，因为每天的数据已经排序

    return result


app = typer.Typer(help="计算加密货币交易的净吃单量", add_completion=False)


def parse_date(date_str: str) -> dt.date:
    """将字符串解析为日期对象。

    Args:
        date_str: 格式为 YYYY-MM-DD 的日期字符串

    Returns:
        解析后的日期对象

    Raises:
        typer.BadParameter: 如果日期格式不正确
    """
    try:
        return dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise typer.BadParameter(
            f"Incorrect date format: {date_str}, please use YYYY-MM-DD format"
        )


@app.command()
def main(
    data_dir: str = typer.Option(..., help="存储交易数据的文件夹路径"),
    symbol: str = typer.Option(..., help="交易对名称，例如 BTCUSDT"),
    start_date: str = typer.Option(
        ..., help="开始日期 (YYYY-MM-DD)", callback=parse_date
    ),
    end_date: str = typer.Option(
        ..., help="结束日期 (YYYY-MM-DD)", callback=parse_date
    ),
    timeframe: TimeFrame = typer.Option(TimeFrame.ONE_HOUR, help="时间范围"),
    output_csv: Optional[Path] = typer.Option(None, help="输出 CSV 文件路径"),
    processes: int = typer.Option(1, help="用于并行处理的进程数"),
) -> None:
    """计算指定日期范围内的净吃单量并输出结果。"""
    console.print(
        f"正在处理 {symbol} 从 {start_date} 到 {end_date} 的数据，使用 {processes} 个进程..."
    )

    t0 = time.time()

    try:
        result = process_date_range(
            data_dir, symbol, start_date, end_date, timeframe, processes
        )

        # 显示结果摘要
        console.print("\n[bold]结果摘要:[/]")
        console.print(f"总记录数: {len(result)}")
        console.print("\n[bold]前 5 条记录:[/]")
        console.print(result.head())
        console.print("\n[bold]后 5 条记录:[/]")
        console.print(result.tail())

        # 如果指定了输出文件，保存为CSV
        if output_csv:
            result.to_csv(output_csv, index=True)
            console.print(f"\n结果已保存到: {output_csv}")

        console.print(f"Tasks completed in {time.time() - t0:.2f} seconds")

    except Exception as e:
        console.print(f"[red]处理错误 {e}[/]")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
