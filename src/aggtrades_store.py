"""Binance aggregated trades data storage management."""

import datetime as dt
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from aggtrades_fetcher import MarketType


class AggTradesStore:
    """Manages storage of Binance aggregated trades data."""

    def __init__(self, base_dir: str = "data"):
        """Initialize the storage manager.

        Args:
            base_dir: Base directory for data storage
        """
        self.base_dir = Path(base_dir)
        self.metadata_dir = self.base_dir / "metadata"
        self.stats_dir = self.metadata_dir / "stats"

        # Create directory structure
        for directory in [self.metadata_dir, self.stats_dir]:
            directory.mkdir(parents=True, exist_ok=True)

    def _get_path_components(
        self, market_type: MarketType, symbol: str, date: dt.date
    ) -> Tuple[Path, str]:
        """获取给定市场类型、交易对和日期的存储路径和文件名。

        Args:
            market_type: 市场类型（现货/合约）
            symbol: 交易对符号
            date: 交易日期

        Returns:
            (目录路径, 文件名)的元组
        """
        directory = (
            self.base_dir
            / market_type.value
            / symbol
            / f"{date.year}"
            / f"{date.month:02d}"
        )

        filename = f"{symbol}_{date:%Y%m%d}.parquet"

        return directory, filename

    def write_trades(
        self,
        market_type: MarketType,
        symbol: str,
        trades_df: pd.DataFrame,
        overwrite: bool = False,
    ) -> None:
        """将交易数据写入存储。

        Args:
            market_type: 市场类型（现货/合约）
            symbol: 交易对符号
            trades_df: 包含交易数据的DataFrame
            overwrite: 是否覆盖现有数据

        Raises:
            ValueError: 如果trades_df为空或结构无效
        """
        if trades_df.empty:
            return

        # 按日分组并写入单独的文件
        for date, day_df in trades_df.groupby(trades_df.timestamp.dt.date):
            directory, filename = self._get_path_components(market_type, symbol, date)
            directory.mkdir(parents=True, exist_ok=True)
            file_path = directory / filename

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

                    # 更新元数据
                    self._update_file_stats(file_path, combined_df)

                    continue

            # 直接存储数据
            pq.write_table(
                table,
                file_path,
                compression="snappy",
            )

            # 更新元数据
            self._update_file_stats(file_path, day_df)

    def _update_file_stats(self, file_path: Path, df: pd.DataFrame) -> None:
        """Update statistics for a data file.

        Args:
            file_path: Path to parquet file
            df: DataFrame containing the file's data
        """
        stats = {
            "min_timestamp": df.timestamp.min().isoformat(),
            "max_timestamp": df.timestamp.max().isoformat(),
            "min_trade_id": int(df.trade_id.min()),
            "max_trade_id": int(df.trade_id.max()),
            "num_trades": len(df),
            "last_modified": dt.datetime.now().isoformat(),
        }

        stats_path = self.stats_dir / f"{file_path.stem}_stats.json"
        with open(stats_path, "w") as f:
            json.dump(stats, f)

    def get_collection_stats(
        self, market_type: Optional[MarketType] = None, symbol: Optional[str] = None
    ) -> dict:
        """获取数据集合的统计信息。

        Args:
            market_type: 可选的市场类型过滤器
            symbol: 可选的交易对符号过滤器

        Returns:
            包含集合统计信息的字典
        """
        stats = {}

        # 扫描所有统计文件
        for stats_file in self.stats_dir.glob("*_stats.json"):
            with open(stats_file) as f:
                file_stats = json.load(f)

            # 从文件名中提取符号
            file_symbol = stats_file.stem.split("_")[0]

            # 尝试从目录结构中确定市场类型
            # 这需要改进，因为现在统计文件可能不包含市场类型信息
            file_market_type = None

            # 如果指定了过滤条件但不匹配，则跳过
            if market_type and file_market_type != market_type:
                continue
            if symbol and file_symbol != symbol:
                continue

            # 创建复合键以区分不同市场类型的相同符号
            key = f"{file_market_type.value if file_market_type else 'unknown'}_{file_symbol}"

            if key not in stats:
                stats[key] = {
                    "market_type": (
                        file_market_type.value if file_market_type else "unknown"
                    ),
                    "symbol": file_symbol,
                    "min_timestamp": file_stats["min_timestamp"],
                    "max_timestamp": file_stats["max_timestamp"],
                    "file_count": 1,
                    "last_updated": file_stats["last_modified"],
                }
            else:
                current = stats[key]
                current["min_timestamp"] = min(
                    current["min_timestamp"], file_stats["min_timestamp"]
                )
                current["max_timestamp"] = max(
                    current["max_timestamp"], file_stats["max_timestamp"]
                )
                current["file_count"] += 1
                current["last_updated"] = max(
                    current["last_updated"], file_stats["last_modified"]
                )

        return stats

    def read_trades(
        self,
        market_type: MarketType,
        symbol: str,
        start_time: dt.datetime,
        end_time: dt.datetime,
    ) -> pd.DataFrame:
        """读取给定时间范围内的交易数据。

        Args:
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
            directory, filename = self._get_path_components(
                market_type, symbol, current_date
            )
            file_path = directory / filename

            if file_path.exists():
                df = pd.read_parquet(file_path)
                # 过滤时间范围
                df = df[(df.timestamp >= start_time) & (df.timestamp < end_time)]
                if not df.empty:
                    dfs.append(df)

            current_date += dt.timedelta(days=1)

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
