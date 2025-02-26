import datetime as dt
from pathlib import Path
from enum import Enum
from typing import Optional, List, Callable

import pandas as pd
import typer


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
        raise FileNotFoundError(f"数据目录不存在: {data_path_by_symbol_date}")

    # 获取所有parquet文件
    files = list(data_path_by_symbol_date.glob("*.parquet"))

    # 检查是否有数据文件
    if not files:
        raise ValueError(f"在 {data_path_by_symbol_date} 中没有找到任何parquet文件")

    # 读取并合并所有文件
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


def process_date_range(
    data_dir: str,
    symbol: str,
    start_date: dt.date,
    end_date: dt.date,
    timeframe: TimeFrame = TimeFrame.ONE_HOUR,
) -> pd.DataFrame:
    """处理指定日期范围内的交易数据并计算净吃单量。

    Args:
        data_dir: 数据目录路径
        symbol: 交易对符号
        start_date: 开始日期
        end_date: 结束日期
        timeframe: 时间框架，默认为1小时

    Returns:
        包含净吃单量的DataFrame
    """
    # 计算日期范围
    date_range = [
        start_date + dt.timedelta(days=i)
        for i in range((end_date - start_date).days + 1)
    ]

    # 读取每一天的数据并合并
    all_data = []
    for date in date_range:
        try:
            df = read_daily_aggtrades(data_dir, symbol, date)
            all_data.append(df)
        except Exception as e:
            typer.echo(f"警告: 无法读取 {date} 的数据: {e}")

    if not all_data:
        typer.echo("错误: 没有找到任何数据")
        raise typer.Exit(code=1)

    # 合并所有日期的数据
    combined_df = pd.concat(all_data)
    combined_df.sort_index(inplace=True)

    # 计算净吃单量
    result = calculate_net_taker_volume(combined_df, timeframe)
    return result


app = typer.Typer(help="计算加密货币交易的净吃单量", add_completion=False)


def parse_date(date_str: str) -> dt.date:
    """将字符串解析为日期对象。

    Args:
        date_str: 格式为 YYYY-MM-DD 的日期字符串

    Returns:
        解析后的日期对象

    Raises:
        ValueError: 当日期格式不正确时抛出
    """
    try:
        return dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise typer.BadParameter(f"日期格式不正确: {date_str}，请使用 YYYY-MM-DD 格式")


@app.command()
def main(
    data_dir: str = typer.Option(..., help="存储交易数据的文件夹路径"),
    symbol: str = typer.Option(..., help="交易对名称，如BTCUSDT"),
    start_date: str = typer.Option(
        ..., help="开始日期 (YYYY-MM-DD)", callback=parse_date
    ),
    end_date: str = typer.Option(
        ..., help="结束日期 (YYYY-MM-DD)", callback=parse_date
    ),
    timeframe: TimeFrame = typer.Option(TimeFrame.ONE_HOUR, help="时间框架"),
    output_csv: Optional[Path] = typer.Option(None, help="输出CSV文件路径"),
) -> None:
    """计算指定日期范围内的净吃单量并输出结果。"""
    typer.echo(f"正在处理 {symbol} 从 {start_date} 到 {end_date} 的数据...")

    try:
        result = process_date_range(data_dir, symbol, start_date, end_date, timeframe)

        # 显示结果摘要
        typer.echo("\n结果摘要:")
        typer.echo(f"总记录数: {len(result)}")
        typer.echo("\n前5条记录:")
        typer.echo(result.head())
        typer.echo("\n后5条记录:")
        typer.echo(result.tail())

        # 如果指定了输出文件，保存为CSV
        if output_csv:
            result.to_csv(output_csv)
            typer.echo(f"\n结果已保存到: {output_csv}")

    except Exception as e:
        typer.echo(f"处理数据时出错: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
