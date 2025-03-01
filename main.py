"""命令行工具用于下载Binance交易所的聚合历史交易数据。"""

import datetime as dt
from typing import Optional

import typer

from src.aggtrades_fetcher import AggTradesFetcherFactory, DataSource, MarketType
from src.aggtrades_store import write_trades

app = typer.Typer(add_completion=False)


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
) -> None:
    """下载Binance交易所的聚合历史交易数据。"""

    # 获取并验证日期
    start = start_date.date()
    end = end_date.date()
    if end < start:
        raise typer.BadParameter("结束日期必须晚于开始日期")

    # 创建数据获取器
    fetcher = AggTradesFetcherFactory.create_fetcher(data_source, market_type)

    # 处理每个交易对
    for symbol in symbols.upper().split(","):
        typer.echo(f"开始下载 {symbol} 的交易数据...")

        current_date = start
        while current_date <= end:
            try:
                typer.echo(f"正在获取 {symbol} 在 {current_date} 的数据...")
                trades_df = fetcher.fetch_daily_trades(symbol, current_date)

                if not trades_df.empty:
                    write_trades(
                        data_dir,
                        market_type,
                        symbol,
                        trades_df,
                        overwrite=override,
                    )
                    typer.echo(f"成功保存 {symbol} 在 {current_date} 的数据")
                else:
                    typer.echo(f"警告: {symbol} 在 {current_date} 没有交易数据")

            except Exception as e:
                typer.echo(
                    f"错误: 下载 {symbol} 在 {current_date} 的数据时失败: {str(e)}"
                )

            current_date += dt.timedelta(days=1)

        typer.echo(f"完成下载 {symbol} 的交易数据")


if __name__ == "__main__":
    app()
