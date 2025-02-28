"""Fetch aggregated trades data from Binance."""

import datetime as dt
import io
import time
import zipfile
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Type

import pandas as pd
import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)


class DataSource(Enum):
    """数据源类型枚举"""

    API = "api"
    HISTORICAL = "historical"


class MarketType(Enum):
    """市场类型枚举"""

    SPOT = "spot"
    FUTURES = "futures"
    COIN_FUTURES = "coin-futures"


class AggTradesFetcher(ABC):
    """聚合交易数据获取器抽象基类"""

    @abstractmethod
    def fetch_daily_trades(self, symbol: str, date: dt.date, **kwargs) -> pd.DataFrame:
        """获取指定日期的聚合交易数据。

        Args:
            symbol: 交易对名称
            date: 日期
            **kwargs: 额外参数

        Returns:
            包含聚合交易数据的DataFrame
        """
        pass


class APIAggTradesFetcher(AggTradesFetcher):
    """通过API获取聚合交易数据"""

    def __init__(self, market_type: MarketType = MarketType.SPOT):
        """初始化API获取器

        Args:
            market_type: 市场类型，默认为现货

        Raises:
            NotImplementedError: 当选择尚未实现的市场类型时
        """
        self.market_type = market_type

        # 检查是否支持该市场类型
        if market_type != MarketType.SPOT:
            raise NotImplementedError(
                f"Market type {market_type.value} not supported yet!"
            )

        self.base_url = self._get_base_url()

    def _get_base_url(self) -> str:
        """根据市场类型获取基础URL

        Returns:
            API基础URL
        """
        if self.market_type == MarketType.SPOT:
            return "https://api.binance.com/api/v3/aggTrades"
        elif self.market_type == MarketType.FUTURES:
            return "https://fapi.binance.com/fapi/v1/aggTrades"
        elif self.market_type == MarketType.COIN_FUTURES:
            return "https://dapi.binance.com/dapi/v1/aggTrades"
        else:
            raise ValueError(f"Invalid market type: {self.market_type}")

    def fetch_hourly_trades(
        self,
        symbol: str,
        date: dt.date,
        hour: int,
        limit: int = 1000,
        request_delay: float = 0.05,
    ) -> pd.DataFrame:
        """获取指定小时的聚合交易数据

        Args:
            symbol: 交易对名称
            date: 日期
            hour: 小时(0-23)
            limit: 每次请求的交易数量限制
            request_delay: 请求间隔时间(秒)

        Returns:
            包含聚合交易数据的DataFrame
        """
        # 转换日期和小时为UTC时间戳范围
        start_dt = dt.datetime.combine(date, dt.time(hour=hour))
        end_dt = start_dt + dt.timedelta(hours=1)

        start_ts = int(start_dt.timestamp() * 1000)
        end_ts = int(end_dt.timestamp() * 1000)

        @retry(
            stop=stop_after_attempt(4),
            wait=wait_exponential(multiplier=1, min=2, max=10),
        )
        def _fetch_trades(start_time: int, end_time: int) -> List[dict]:
            """带重试逻辑的交易数据获取辅助函数"""
            params = {
                "symbol": symbol,
                "startTime": start_time,
                "endTime": end_time,
                "limit": limit,
            }

            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            return response.json()

        all_trades = []
        current_end = end_ts

        while True:
            trades = _fetch_trades(start_ts, current_end)
            if not trades:
                break

            # 按时间戳升序排序
            trades.sort(key=lambda x: x["T"])

            # 添加到集合中
            all_trades.extend(trades)

            if len(trades) < limit:
                # 少于限制数量意味着已获取所有交易
                break

            # 更新开始时间戳以获取下一批
            # 使用最后一个交易的时间戳+1毫秒避免重复
            start_ts = trades[-1]["T"] + 1

            if start_ts >= end_ts:
                break

            time.sleep(request_delay)

        if not all_trades:
            return pd.DataFrame()

        # 转换为DataFrame
        df = pd.DataFrame(all_trades)
        df = df.rename(
            columns={
                "a": "trade_id",
                "T": "timestamp",
                "p": "price",
                "q": "quantity",
                "m": "is_buyer_maker",
            }
        )

        # 转换数据类型
        df["trade_id"] = df["trade_id"].astype("int64")
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df["price"] = df["price"].astype("float64")
        df["quantity"] = df["quantity"].astype("float64")
        df["is_buyer_maker"] = df["is_buyer_maker"].astype("bool")

        # 按时间戳排序
        df = df.sort_values("timestamp")

        # 确保交易严格在请求的小时内
        df = df[
            (df["timestamp"] >= pd.Timestamp(start_dt, tz="UTC"))
            & (df["timestamp"] < pd.Timestamp(end_dt, tz="UTC"))
        ]

        return df[["trade_id", "timestamp", "price", "quantity", "is_buyer_maker"]]

    def fetch_daily_trades(
        self, symbol: str, date: dt.date, limit: int = 1000, request_delay: float = 0.05
    ) -> pd.DataFrame:
        """获取指定日期的所有聚合交易数据

        Args:
            symbol: 交易对名称
            date: 日期
            limit: 每次请求的交易数量限制
            request_delay: 请求间隔时间(秒)

        Returns:
            包含聚合交易数据的DataFrame
        """
        all_trades = []

        for hour in range(24):
            df = self.fetch_hourly_trades(symbol, date, hour, limit, request_delay)
            all_trades.append(df)

        return (
            pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
        )


class HistoricalAggTradesFetcher(AggTradesFetcher):
    """从历史数据仓库获取聚合交易数据"""

    def __init__(self, market_type: MarketType = MarketType.SPOT):
        """初始化历史数据获取器

        Args:
            market_type: 市场类型，默认为现货

        Raises:
            NotImplementedError: 当选择尚未实现的市场类型时
        """
        self.market_type = market_type

        # 检查是否支持该市场类型
        if market_type != MarketType.SPOT:
            raise NotImplementedError(
                f"Market type {market_type.value} not supported yet!"
            )

        self.base_url = self._get_base_url()

    def _get_base_url(self) -> str:
        """根据市场类型获取基础URL

        Returns:
            历史数据仓库基础URL
        """
        if self.market_type == MarketType.SPOT:
            return "https://data.binance.vision/data/spot/daily/aggTrades"
        elif self.market_type == MarketType.FUTURES:
            return "https://data.binance.vision/data/futures/um/daily/aggTrades"
        elif self.market_type == MarketType.COIN_FUTURES:
            return "https://data.binance.vision/data/futures/cm/daily/aggTrades"
        else:
            raise ValueError(f"Invalid market type: {self.market_type}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(
            (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
            )
        ),
    )
    def fetch_daily_trades(self, symbol: str, date: dt.date) -> pd.DataFrame:
        """从Binance历史数据仓库下载聚合交易数据

        下载指定交易对和日期的历史聚合交易数据，解压并转换为DataFrame格式。
        数据来源于Binance公共数据仓库。

        Args:
            symbol: 交易对名称，例如 'BTCUSDT'
            date: 需要下载数据的日期

        Returns:
            包含聚合交易数据的DataFrame，列包括:
            - trade_id: 聚合交易ID
            - timestamp: 交易时间戳（UTC）
            - price: 交易价格
            - quantity: 交易数量
            - is_buyer_maker: 买方是否为挂单方

        Raises:
            requests.exceptions.HTTPError: 当请求失败或文件不存在时（如404错误）
            zipfile.BadZipFile: 当下载的文件不是有效的zip文件时
            ValueError: 当解析数据失败时
            FileNotFoundError: 当zip文件中找不到CSV文件时
        """
        # 构建URL
        date_str = date.strftime("%Y-%m-%d")
        filename = f"{symbol}-aggTrades-{date_str}.zip"
        url = f"{self.base_url}/{symbol}/{filename}"

        # 下载文件
        response = requests.get(url, stream=True)
        response.raise_for_status()  # 如果是404等错误，这里会抛出异常，不会重试

        # 使用内存中的BytesIO对象处理zip文件，避免写入磁盘
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            # 获取zip文件中的CSV文件名（通常只有一个文件）
            csv_files = [name for name in zip_file.namelist() if name.endswith(".csv")]
            csv_filename = csv_files[0]

            # 读取CSV文件内容
            with zip_file.open(csv_filename) as csv_file:
                # 读取CSV数据，注意没有表头
                df = pd.read_csv(
                    csv_file,
                    header=None,
                    names=[
                        "trade_id",
                        "price",
                        "quantity",
                        "first_trade_id",
                        "last_trade_id",
                        "timestamp",
                        "is_buyer_maker",
                        "is_best_price_match",
                    ],
                )

        # 处理日期字段，时间戳精确到微妙
        df["timestamp"] = pd.to_datetime(
            df["timestamp"].astype("int64"), unit="us", utc=True
        )

        # 只保留与API获取器输出一致的列
        return df[["trade_id", "timestamp", "price", "quantity", "is_buyer_maker"]]


class AggTradesFetcherFactory:
    """聚合交易数据获取器工厂"""

    _fetchers: Dict[DataSource, Dict[MarketType, Type[AggTradesFetcher]]] = {
        DataSource.API: {
            MarketType.SPOT: APIAggTradesFetcher,
            MarketType.FUTURES: APIAggTradesFetcher,
            MarketType.COIN_FUTURES: APIAggTradesFetcher,
        },
        DataSource.HISTORICAL: {
            MarketType.SPOT: HistoricalAggTradesFetcher,
            MarketType.FUTURES: HistoricalAggTradesFetcher,
            MarketType.COIN_FUTURES: HistoricalAggTradesFetcher,
        },
    }

    @classmethod
    def create_fetcher(
        cls, data_source: DataSource, market_type: MarketType = MarketType.SPOT
    ) -> AggTradesFetcher:
        """创建聚合交易数据获取器。

        Args:
            data_source: 数据源类型
            market_type: 市场类型，默认为现货

        Returns:
            聚合交易数据获取器实例

        Raises:
            ValueError: 当不支持的数据源或市场类型时
        """
        try:
            fetcher_class = cls._fetchers[data_source][market_type]
            return fetcher_class(market_type=market_type)
        except KeyError:
            raise ValueError(
                f"Invalid market type or source: {data_source}, {market_type}"
            )
