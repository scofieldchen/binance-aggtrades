"""命令行工具用于下载Binance交易所的聚合历史交易数据。"""

import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple

import typer
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from src.aggtrades_fetcher import (
    AggTradesFetcherFactory,
    DataSource,
    MarketType,
    AggTradesFetcher,
)
from src.aggtrades_store import write_trades

app = typer.Typer(add_completion=False)


def process_single_day(
    symbol: str,
    date: dt.date,
    fetcher: AggTradesFetcher,
    data_dir: str,
    market_type: MarketType,
    override: bool,
) -> Tuple[bool, str]:
    """处理单个交易日的数据下载。

    Args:
        symbol: 交易对名称
        date: 日期
        fetcher: 数据获取器
        data_dir: 数据存储目录
        market_type: 市场类型
        override: 是否覆盖现有数据

    Returns:
        (成功标志, 消息)元组
    """
    try:
        trades_df = fetcher.fetch_daily_trades(symbol, date)

        if not trades_df.empty:
            write_trades(
                data_dir,
                market_type,
                symbol,
                trades_df,
                overwrite=override,
            )
            return True, f"成功保存 {symbol} 在 {date} 的数据"
        else:
            return True, f"警告: {symbol} 在 {date} 没有交易数据"

    except Exception as e:
        return False, f"错误: 下载 {symbol} 在 {date} 的数据时失败: {str(e)}"


@app.command()
def download(
    symbols: str = typer.Option(
        ..., "--symbols", "-s", help="交易对列表，用逗号分隔，例如: BTCUSDT,ETHUSDT"
    ),
    start_date: dt.datetime = typer.Option(
        ...,
        "--start-date",
        "-sd",
        help="开始日期 (YYYYMMDD 或 YYYY-MM-DD)",
        formats=["%Y-%m-%d", "%Y%m%d"],
    ),
    end_date: dt.datetime = typer.Option(
        ...,
        "--end-date",
        "-ed",
        help="结束日期 (YYYYMMDD 或 YYYY-MM-DD)",
        formats=["%Y-%m-%d", "%Y%m%d"],
    ),
    market_type: MarketType = typer.Option(
        MarketType.SPOT,
        "--market-type",
        "-m",
        help="市场类型（现货或合约）",
    ),
    data_source: DataSource = typer.Option(
        DataSource.HISTORICAL,
        "--source",
        help="数据源类型（API或历史数据）",
    ),
    data_dir: str = typer.Option(
        "data",
        "--data-dir",
        "-d",
        help="数据存储目录路径",
    ),
    override: bool = typer.Option(
        True,
        "--override/--no-override",
        help="是否覆盖现有数据",
    ),
    threads: int = typer.Option(
        3,
        "--threads",
        "-t",
        help="下载线程数",
        min=1,
        max=10,
    ),
) -> None:
    """下载Binance交易所的聚合历史交易数据。"""

    # 获取并验证日期
    start = start_date.date()
    end = end_date.date()
    if end < start:
        raise typer.BadParameter("结束日期必须晚于开始日期")

    # 创建数据获取器
    fetcher = AggTradesFetcherFactory.create_fetcher(data_source, market_type)

    # 生成所有需要处理的任务
    tasks: List[Tuple[str, dt.date]] = []
    for symbol in symbols.upper().split(","):
        current_date = start
        while current_date <= end:
            tasks.append((symbol, current_date))
            current_date += dt.timedelta(days=1)

    # 使用Rich创建进度显示
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
    ) as progress:
        # 创建总进度任务
        total_task = progress.add_task(f"[cyan]下载数据中...", total=len(tasks))

        # 使用线程池执行下载任务
        with ThreadPoolExecutor(max_workers=threads) as executor:
            # 提交所有任务
            future_to_task = {
                executor.submit(
                    process_single_day,
                    symbol,
                    date,
                    fetcher,
                    data_dir,
                    market_type,
                    override,
                ): (symbol, date)
                for symbol, date in tasks
            }

            # 处理完成的任务
            for future in as_completed(future_to_task):
                symbol, date = future_to_task[future]
                success, message = future.result()

                # 更新进度
                progress.advance(total_task)

                if not success:
                    typer.echo(message, err=True)

    typer.echo("所有数据下载完成！")


if __name__ == "__main__":
    app()
