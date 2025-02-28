import os
import sys
import datetime as dt

sys.path.insert(0, "/users/scofield/binance-aggtrades/src")

from aggtrades_fetcher import DataSource, MarketType, AggTradesFetcherFactory


# 测试从API获取数据
# api_fetcher = AggTradesFetcherFactory.create_fetcher(DataSource.API, MarketType.SPOT)
# df = api_fetcher.fetch_daily_trades("BTCUSDT", dt.date(2025, 2, 15))
# print(df.head())
# print(df.tail())
# print(df.info())

# 测试从数据仓库获取数据
# historical_fetcher = AggTradesFetcherFactory.create_fetcher(
#     DataSource.HISTORICAL, MarketType.SPOT
# )
# df = historical_fetcher.fetch_daily_trades("ETHUSDT", dt.date(2025, 2, 15))
# print(df.head())
# print(df.tail())
# print(df.info())
