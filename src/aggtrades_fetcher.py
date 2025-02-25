"""Fetch aggregated trades data from Binance."""

import datetime as dt
import time
from typing import List

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential


def get_hourly_agg_trades(
    symbol: str,
    date: dt.date,
    hour: int,
    limit: int = 1000,
    request_delay: float = 0.05,
) -> pd.DataFrame:
    """Fetch all aggregated trades for a symbol in a specific hour.

    Args:
        symbol: Trading pair symbol (e.g. 'BTCUSDT')
        date: UTC date to fetch trades for
        hour: Hour of the day (0-23) to fetch trades for
        limit: Number of trades to fetch per request
        request_delay: Delay between API requests in seconds

    Returns:
        DataFrame containing the aggregated trades with columns:
        - trade_id: Aggregate trade ID
        - timestamp: Trade timestamp (UTC)
        - price: Trade price
        - quantity: Trade quantity
        - is_buyer_maker: Whether the buyer was the maker

    Raises:
        requests.exceptions.RequestException: If API request fails after retries
    """
    # Convert date and hour to UTC timestamp range
    start_dt = dt.datetime.combine(date, dt.time(hour=hour))
    end_dt = start_dt + dt.timedelta(hours=1)

    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)

    @retry(
        stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def _fetch_trades(start_time: int, end_time: int) -> List[dict]:
        """Helper function to fetch trades with retry logic."""
        params = {
            "symbol": symbol,
            "startTime": start_time,
            "endTime": end_time,
            "limit": limit,
        }

        response = requests.get(
            "https://api.binance.com/api/v3/aggTrades", params=params
        )
        response.raise_for_status()
        return response.json()

    all_trades = []
    current_end = end_ts

    while True:
        trades = _fetch_trades(start_ts, current_end)
        if not trades:
            break

        # Sort by timestamp ascending
        trades.sort(key=lambda x: x["T"])

        # Add to our collection
        all_trades.extend(trades)

        if len(trades) < limit:
            # Less than limit means we got all trades
            break

        # Update start timestamp to get next batch
        # Use the last trade's timestamp + 1ms to avoid duplicates
        start_ts = trades[-1]["T"] + 1

        if start_ts >= end_ts:
            break

        time.sleep(request_delay)

    if not all_trades:
        return pd.DataFrame()

    # Convert to DataFrame
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

    # Convert types
    df["trade_id"] = df["trade_id"].astype("int64")
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["price"] = df["price"].astype("float64")
    df["quantity"] = df["quantity"].astype("float64")
    df["is_buyer_maker"] = df["is_buyer_maker"].astype("bool")

    # Sort by timestamp
    df = df.sort_values("timestamp")

    # Ensure trades are strictly within the requested hour
    df = df[
        (df["timestamp"] >= pd.Timestamp(start_dt, tz="UTC"))
        & (df["timestamp"] < pd.Timestamp(end_dt, tz="UTC"))
    ]

    return df[["trade_id", "timestamp", "price", "quantity", "is_buyer_maker"]]


def get_daily_agg_trades(
    symbol: str,
    date: dt.date,
    limit: int = 1000,
    request_delay: float = 0.05,
) -> pd.DataFrame:
    """Fetch all aggregated trades for a symbol for an entire day by hour."""
    all_trades = []

    for hour in range(24):
        df = get_hourly_agg_trades(symbol, date, hour, limit, request_delay)
        all_trades.append(df)

    return pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
