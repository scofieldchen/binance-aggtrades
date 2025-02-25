"""Binance aggregated trades data storage management."""

import datetime as dt
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pybloom_live import ScalableBloomFilter


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
        self.wal_dir = self.metadata_dir / "wal"

        # Create directory structure
        for directory in [self.metadata_dir, self.stats_dir, self.wal_dir]:
            directory.mkdir(parents=True, exist_ok=True)

        # Initialize bloom filter for trade_id lookups
        self.trade_id_filter = ScalableBloomFilter(
            mode=ScalableBloomFilter.SMALL_SET_GROWTH
        )

        # Load existing metadata
        self._load_metadata()

    def _get_path_components(
        self, symbol: str, timestamp: dt.datetime
    ) -> Tuple[Path, str]:
        """Get storage path and filename for given symbol and timestamp.

        Args:
            symbol: Trading pair symbol
            timestamp: Trade timestamp

        Returns:
            Tuple of (directory_path, filename)
        """
        directory = (
            self.base_dir
            / f"symbol={symbol}"
            / f"year={timestamp.year}"
            / f"month={timestamp.month:02d}"
            / f"day={timestamp.day:02d}"
        )

        filename = f"{symbol}_{timestamp:%Y%m%d_%H}.parquet"

        return directory, filename

    def write_trades(
        self, symbol: str, trades_df: pd.DataFrame, overwrite: bool = False
    ) -> None:
        """Write trades data to storage.

        Args:
            symbol: Trading pair symbol
            trades_df: DataFrame containing trades data
            overwrite: Whether to overwrite existing data

        Raises:
            ValueError: If trades_df is empty or has invalid structure
        """
        if trades_df.empty:
            return

        # Group by hour and write separate files
        for timestamp, hour_df in trades_df.groupby(trades_df.timestamp.dt.floor("h")):
            directory, filename = self._get_path_components(symbol, timestamp)
            directory.mkdir(parents=True, exist_ok=True)

            file_path = directory / filename

            # Write WAL entry
            wal_entry = {
                "timestamp": timestamp.isoformat(),
                "symbol": symbol,
                "path": str(file_path),
                "num_trades": len(hour_df),
            }
            self._write_wal(wal_entry)

            # Convert to Arrow table
            table = pa.Table.from_pandas(hour_df)

            # Check if file exists and handle overwrite
            if file_path.exists():
                if overwrite:
                    file_path.unlink()  # Delete existing file
                else:
                    continue  # Skip if file exists and overwrite is False

            # Write parquet file with simpler parameters
            pq.write_table(
                table,
                file_path,
                compression="snappy",
            )

            # Update metadata
            self._update_file_stats(file_path, hour_df)

            # Update bloom filter
            for trade_id in hour_df.trade_id:
                self.trade_id_filter.add(trade_id)

    def _write_wal(self, entry: Dict) -> None:
        """Write entry to WAL file.

        Args:
            entry: Dictionary containing WAL entry data
        """
        wal_path = self.wal_dir / f"wal_{dt.datetime.now():%Y%m%d_%H%M%S_%f}.json"
        with open(wal_path, "w") as f:
            json.dump(entry, f)

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

    def _load_metadata(self) -> None:
        """Load existing metadata and rebuild indices."""
        # Load existing trade IDs into bloom filter
        for stats_file in self.stats_dir.glob("*_stats.json"):
            with open(stats_file) as f:
                stats = json.load(f)
                for trade_id in range(stats["min_trade_id"], stats["max_trade_id"] + 1):
                    self.trade_id_filter.add(trade_id)

    def get_collection_stats(self, symbol: Optional[str] = None) -> dict:
        """Get statistics about the data collection.

        Args:
            symbol: Optional symbol to filter stats

        Returns:
            Dictionary containing collection statistics
        """
        stats = {}

        # Scan all stats files
        for stats_file in self.stats_dir.glob("*_stats.json"):
            with open(stats_file) as f:
                file_stats = json.load(f)

            # Extract symbol from filename
            file_symbol = stats_file.stem.split("_")[0]

            if symbol and file_symbol != symbol:
                continue

            if file_symbol not in stats:
                stats[file_symbol] = {
                    "min_timestamp": file_stats["min_timestamp"],
                    "max_timestamp": file_stats["max_timestamp"],
                    "file_count": 1,
                    "last_updated": file_stats["last_modified"],
                }
            else:
                current = stats[file_symbol]
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
        self, symbol: str, start_time: dt.datetime, end_time: dt.datetime
    ) -> pd.DataFrame:
        """Read trades data for given time range.

        Args:
            symbol: Trading pair symbol
            start_time: Start timestamp (inclusive)
            end_time: End timestamp (exclusive)

        Returns:
            DataFrame containing trades data
        """
        dfs = []
        current_time = start_time

        while current_time < end_time:
            directory, filename = self._get_path_components(symbol, current_time)
            file_path = directory / filename

            if file_path.exists():
                df = pd.read_parquet(file_path)
                df = df[(df.timestamp >= start_time) & (df.timestamp < end_time)]
                dfs.append(df)

            current_time += dt.timedelta(hours=1)

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
