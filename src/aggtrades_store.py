"""Binance aggregated trades data storage management."""

import datetime as dt
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from aggtrades_fetcher import MarketType


def get_file_path(
    base_dir: str, market_type: MarketType, symbol: str, date: dt.date
) -> Path:
    """获取给定市场类型、交易对和日期的存储文件路径。

    Args:
        base_dir: 数据存储的基础目录
        market_type: 市场类型（现货/合约）
        symbol: 交易对符号
        date: 交易日期

    Returns:
        完整的文件路径
    """
    return (
        Path(base_dir)
        / market_type.value
        / symbol
        / f"{date.year}"
        / f"{date.month:02d}"
        / f"{symbol}_{date:%Y%m%d}.parquet"
    )


def write_trades(
    base_dir: str,
    market_type: MarketType,
    symbol: str,
    trades_df: pd.DataFrame,
    overwrite: bool = False,
) -> None:
    """将交易数据写入存储。

    Args:
        base_dir: 数据存储的基础目录
        market_type: 市场类型（现货/合约）
        symbol: 交易对符号
        trades_df: 包含交易数据的DataFrame
        overwrite: 是否覆盖现有数据

    Raises:
        ValueError: 如果trades_df为空或结构无效
    """
    if trades_df.empty:
        return

    # 确保基础目录存在
    Path(base_dir).mkdir(parents=True, exist_ok=True)

    # 按日分组并写入单独的文件
    for date, day_df in trades_df.groupby(trades_df.timestamp.dt.date):
        file_path = get_file_path(base_dir, market_type, symbol, date)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # 转换为Arrow表
        table = pa.Table.from_pandas(day_df)

        # 检查文件是否存在并处理覆盖
        if file_path.exists():
            if overwrite:
                file_path.unlink()  # 删除现有文件
            else:
                # 如果不覆盖但需要追加，则读取现有数据，合并后重写
                existing_df = pd.read_parquet(file_path)
                combined_df = pd.concat([existing_df, day_df]).drop_duplicates(
                    subset=["trade_id"]
                )
                combined_df = combined_df.sort_values("timestamp")
                table = pa.Table.from_pandas(combined_df)
                pq.write_table(table, file_path, compression="snappy")
                continue

        # 直接存储数据
        pq.write_table(
            table,
            file_path,
            compression="snappy",
        )


def read_trades(
    base_dir: str,
    market_type: MarketType,
    symbol: str,
    start_time: dt.datetime,
    end_time: dt.datetime,
) -> pd.DataFrame:
    """读取给定时间范围内的交易数据。

    Args:
        base_dir: 数据存储的基础目录
        market_type: 市场类型
        symbol: 交易对符号
        start_time: 开始时间戳（包含）
        end_time: 结束时间戳（不包含）

    Returns:
        包含交易数据的DataFrame
    """
    dfs = []

    # 计算日期范围
    start_date = start_time.date()
    end_date = end_time.date()

    # 如果结束时间有时分秒，需要包含结束日期
    if end_time.time() != dt.time(0, 0, 0):
        end_date += dt.timedelta(days=1)

    current_date = start_date

    while current_date < end_date:
        file_path = get_file_path(base_dir, market_type, symbol, current_date)

        if file_path.exists():
            df = pd.read_parquet(file_path)
            df = df[(df.timestamp >= start_time) & (df.timestamp < end_time)]
            if not df.empty:
                dfs.append(df)

        current_date += dt.timedelta(days=1)

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
