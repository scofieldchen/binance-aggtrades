"""测试 aggtrades_store 模块的功能。"""

import datetime as dt
import shutil
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.aggtrades_fetcher import MarketType
from src.aggtrades_store import get_file_path, read_trades, write_trades


@pytest.fixture
def test_data_dir(tmp_path):
    """创建测试数据目录。

    Args:
        tmp_path: pytest 提供的临时目录路径

    Returns:
        临时测试数据目录的路径
    """
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    yield str(test_dir)
    # 测试后清理
    if test_dir.exists():
        shutil.rmtree(test_dir)


@pytest.fixture
def sample_trades_df():
    """创建样本交易数据。

    Returns:
        包含样本交易数据的 DataFrame
    """
    data = {
        "trade_id": [1, 2, 3, 4, 5],
        "price": [100.0, 101.0, 99.5, 100.5, 102.0],
        "qty": [1.0, 2.0, 0.5, 1.5, 1.0],
        "timestamp": [
            dt.datetime(2023, 1, 1, 10, 0, 0),
            dt.datetime(2023, 1, 1, 10, 5, 0),
            dt.datetime(2023, 1, 1, 10, 10, 0),
            dt.datetime(2023, 1, 1, 10, 15, 0),
            dt.datetime(2023, 1, 1, 10, 20, 0),
        ],
        "is_buyer_maker": [True, False, True, False, True],
    }
    return pd.DataFrame(data)


def test_get_file_path():
    """测试 get_file_path 函数。"""
    # 测试参数
    base_dir = "/data"
    market_type = MarketType.SPOT
    symbol = "BTCUSDT"
    date = dt.date(2023, 1, 15)

    # 执行函数
    path = get_file_path(base_dir, market_type, symbol, date)

    # 验证结果
    expected_path = Path("/data/spot/BTCUSDT/2023/01/BTCUSDT_20230115.parquet")
    assert path == expected_path


def test_write_and_read_trades(test_data_dir, sample_trades_df):
    """测试写入和读取交易数据。

    Args:
        test_data_dir: 测试数据目录
        sample_trades_df: 样本交易数据
    """
    # 测试参数
    market_type = MarketType.SPOT
    symbol = "BTCUSDT"

    # 写入数据
    write_trades(test_data_dir, market_type, symbol, sample_trades_df)

    # 验证文件是否创建
    date = dt.date(2023, 1, 1)
    file_path = get_file_path(test_data_dir, market_type, symbol, date)
    assert file_path.exists()

    # 读取数据
    start_time = dt.datetime(2023, 1, 1, 10, 0, 0)
    end_time = dt.datetime(2023, 1, 1, 10, 30, 0)
    read_df = read_trades(test_data_dir, market_type, symbol, start_time, end_time)

    # 验证读取的数据
    assert len(read_df) == len(sample_trades_df)
    assert set(read_df.trade_id) == set(sample_trades_df.trade_id)


def test_write_trades_overwrite(test_data_dir, sample_trades_df):
    """测试覆盖写入交易数据。

    Args:
        test_data_dir: 测试数据目录
        sample_trades_df: 样本交易数据
    """
    # 测试参数
    market_type = MarketType.SPOT
    symbol = "BTCUSDT"

    # 首次写入数据
    write_trades(test_data_dir, market_type, symbol, sample_trades_df)

    # 创建新数据（不同的交易ID）
    new_data = sample_trades_df.copy()
    new_data["trade_id"] = new_data["trade_id"] + 100

    # 覆盖写入
    write_trades(test_data_dir, market_type, symbol, new_data, overwrite=True)

    # 读取数据
    start_time = dt.datetime(2023, 1, 1, 10, 0, 0)
    end_time = dt.datetime(2023, 1, 1, 10, 30, 0)
    read_df = read_trades(test_data_dir, market_type, symbol, start_time, end_time)

    # 验证只有新数据
    assert len(read_df) == len(new_data)
    assert set(read_df.trade_id) == set(new_data.trade_id)
    assert not any(
        tid in read_df.trade_id.values for tid in sample_trades_df.trade_id.values
    )


def test_write_trades_append(test_data_dir, sample_trades_df):
    """测试追加写入交易数据。

    Args:
        test_data_dir: 测试数据目录
        sample_trades_df: 样本交易数据
    """
    # 测试参数
    market_type = MarketType.SPOT
    symbol = "BTCUSDT"

    # 首次写入数据
    write_trades(test_data_dir, market_type, symbol, sample_trades_df)

    # 创建新数据（不同的交易ID）
    new_data = sample_trades_df.copy()
    new_data["trade_id"] = new_data["trade_id"] + 100

    # 追加写入
    write_trades(test_data_dir, market_type, symbol, new_data, overwrite=False)

    # 读取数据
    start_time = dt.datetime(2023, 1, 1, 10, 0, 0)
    end_time = dt.datetime(2023, 1, 1, 10, 30, 0)
    read_df = read_trades(test_data_dir, market_type, symbol, start_time, end_time)

    # 验证包含所有数据
    assert len(read_df) == len(sample_trades_df) + len(new_data)
    assert set(read_df.trade_id) == set(sample_trades_df.trade_id) | set(
        new_data.trade_id
    )


def test_read_trades_empty(test_data_dir):
    """测试读取不存在的交易数据。

    Args:
        test_data_dir: 测试数据目录
    """
    # 测试参数
    market_type = MarketType.SPOT
    symbol = "NONEXISTENT"
    start_time = dt.datetime(2023, 1, 1, 10, 0, 0)
    end_time = dt.datetime(2023, 1, 1, 10, 30, 0)

    # 读取数据
    read_df = read_trades(test_data_dir, market_type, symbol, start_time, end_time)

    # 验证返回空DataFrame
    assert read_df.empty


def test_read_trades_time_filter(test_data_dir, sample_trades_df):
    """测试按时间过滤读取交易数据。

    Args:
        test_data_dir: 测试数据目录
        sample_trades_df: 样本交易数据
    """
    # 测试参数
    market_type = MarketType.SPOT
    symbol = "BTCUSDT"

    # 写入数据
    write_trades(test_data_dir, market_type, symbol, sample_trades_df)

    # 读取部分时间范围的数据
    start_time = dt.datetime(2023, 1, 1, 10, 5, 0)
    end_time = dt.datetime(2023, 1, 1, 10, 15, 0)
    read_df = read_trades(test_data_dir, market_type, symbol, start_time, end_time)

    # 验证只读取了指定时间范围的数据
    assert len(read_df) == 2
    assert set(read_df.trade_id) == {2, 3}
