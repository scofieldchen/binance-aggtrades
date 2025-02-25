"""Command line interface for Binance aggregated trades data management."""

import datetime as dt
import json
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
)
from rich.table import Table

from aggtrades_store import AggTradesStore
from aggtrades_fetcher import get_hourly_agg_trades

app = typer.Typer(help="Binance aggregated trades data management tool")
console = Console()


class DownloadTracker:
    """Track download progress and status."""

    def __init__(self):
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
        )
        self.overall_progress = self.progress.add_task("Overall Progress", total=100)
        self.symbol_progress: Optional[TaskID] = None
        self.failed_tasks: List[dict] = []
        self.total_size = 0
        self.processed_size = 0

    def update_stats(self, symbol: str, date: dt.date, size: int):
        """Update download statistics."""
        self.processed_size += size
        self.progress.update(
            self.symbol_progress,
            description=f"Downloading {symbol} ({date})",
            advance=1,
        )
        if self.total_size > 0:
            overall_progress = (self.processed_size / self.total_size) * 100
            self.progress.update(self.overall_progress, completed=overall_progress)


class RetryManager:
    """Manage failed tasks for retry."""

    def __init__(self, retry_file: Path = Path(".retry")):
        self.retry_file = retry_file

    def save(self, tasks: List[dict]):
        """Save failed tasks to file."""
        with open(self.retry_file, "w") as f:
            json.dump(tasks, f)

    def load(self) -> List[dict]:
        """Load failed tasks from file."""
        if not self.retry_file.exists():
            return []
        with open(self.retry_file) as f:
            return json.load(f)

    def clear(self):
        """Clear retry file."""
        if self.retry_file.exists():
            self.retry_file.unlink()


@app.command()
def download(
    symbols: List[str],
    start_date: str = typer.Argument(..., help="Start date (YYYY-MM-DD)"),
    end_date: str = typer.Argument(..., help="End date (YYYY-MM-DD)"),
    data_dir: Path = typer.Option("data", help="Data directory path"),
):
    """Download aggregated trades for specified symbols and date range."""
    # Convert string dates to datetime objects
    start_dt = dt.datetime.strptime(start_date, "%Y-%m-%d").replace(
        tzinfo=dt.timezone.utc
    )
    end_dt = dt.datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)

    store = AggTradesStore(str(data_dir))
    tracker = DownloadTracker()
    retry_manager = RetryManager()

    # Calculate total workload
    days_per_symbol = (end_dt.date() - start_dt.date()).days + 1
    total_tasks = len(symbols) * days_per_symbol * 24  # 24 hours per day
    tracker.total_size = total_tasks

    with Live(tracker.progress, refresh_per_second=5):
        for symbol in symbols:
            tracker.symbol_progress = tracker.progress.add_task(
                f"Downloading {symbol}...",
                total=days_per_symbol * 24,
            )

            current_date = start_dt.date()
            while current_date <= end_dt.date():
                for hour in range(24):
                    try:
                        # Check if data exists
                        hour_start = dt.datetime.combine(
                            current_date, dt.time(hour=hour), tzinfo=dt.timezone.utc
                        )
                        hour_end = hour_start + dt.timedelta(hours=1)

                        existing_data = store.read_trades(symbol, hour_start, hour_end)

                        if existing_data.empty:
                            df = get_hourly_agg_trades(symbol, current_date, hour)
                            if not df.empty:
                                store.write_trades(symbol, df)
                                tracker.update_stats(symbol, current_date, len(df))
                        else:
                            tracker.update_stats(symbol, current_date, 0)

                    except Exception as e:
                        console.print(
                            f"[red]Error downloading {symbol} for {current_date} hour {hour}: {e}"
                        )
                        tracker.failed_tasks.append(
                            {
                                "symbol": symbol,
                                "date": current_date.isoformat(),
                                "hour": hour,
                                "error": str(e),
                            }
                        )

                current_date += dt.timedelta(days=1)

    if tracker.failed_tasks:
        retry_manager.save(tracker.failed_tasks)
        console.print(
            f"[yellow]Some tasks failed. Retry file saved to {retry_manager.retry_file}"
        )


@app.command()
def update(
    symbols: List[str],
    force: bool = typer.Option(False, help="Force full update"),
    data_dir: Path = typer.Option("data", help="Data directory path"),
):
    """Update data to latest available."""
    store = AggTradesStore(str(data_dir))
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TimeRemainingColumn(),
    )

    with Live(progress):
        for symbol in symbols:
            if force:
                # Force update mode: update last 7 days
                end_time = dt.datetime.now(dt.timezone.utc)
                start_time = end_time - dt.timedelta(days=7)
            else:
                # Incremental update: get latest timestamp
                end_time = dt.datetime.now(dt.timezone.utc)
                start_time = end_time - dt.timedelta(days=1)
                try:
                    existing_data = store.read_trades(symbol, start_time, end_time)
                    if not existing_data.empty:
                        start_time = existing_data.timestamp.max()
                except Exception as e:
                    console.print(f"[red]Error reading data for {symbol}: {e}")
                    continue

            task_id = progress.add_task(
                f"Updating {symbol}",
                total=(end_time.date() - start_time.date()).days + 1,
            )

            current_date = start_time.date()
            while current_date <= end_time.date():
                for hour in range(24):
                    try:
                        df = get_hourly_agg_trades(symbol, current_date, hour)
                        if not df.empty:
                            store.write_trades(symbol, df, overwrite=force)
                    except Exception as e:
                        console.print(
                            f"[red]Error updating {symbol} for {current_date} hour {hour}: {e}"
                        )

                current_date += dt.timedelta(days=1)
                progress.advance(task_id)


@app.command()
def query(
    data_dir: Path = typer.Option("data", help="Data directory path"),
    symbol: Optional[str] = typer.Option(None, help="Filter by symbol"),
):
    """Query data collection statistics."""
    store = AggTradesStore(str(data_dir))

    with console.status("[bold blue]Analyzing data collection..."):
        stats = store.get_collection_stats(symbol)

    if not stats:
        console.print("[yellow]No data found")
        return

    # Create summary table
    table = Table(title="Data Collection Statistics")
    table.add_column("Symbol", style="cyan")
    table.add_column("Start Date", style="green")
    table.add_column("End Date", style="green")
    table.add_column("Files", justify="right", style="blue")
    table.add_column("Last Updated", style="magenta")

    # Add rows sorted by symbol
    for symbol in sorted(stats.keys()):
        symbol_stats = stats[symbol]
        start_date = dt.datetime.fromisoformat(symbol_stats["min_timestamp"]).strftime(
            "%Y-%m-%d"
        )
        end_date = dt.datetime.fromisoformat(symbol_stats["max_timestamp"]).strftime(
            "%Y-%m-%d"
        )
        last_updated = dt.datetime.fromisoformat(symbol_stats["last_updated"]).strftime(
            "%Y-%m-%d %H:%M"
        )

        table.add_row(
            symbol,
            start_date,
            end_date,
            f"{symbol_stats['file_count']:,}",
            last_updated,
        )

    # Add summary footer
    table.add_section()
    table.add_row(
        f"Total ({len(stats)} symbols)",
        "",
        "",
        f"{sum(s['file_count'] for s in stats.values()):,}",
        "",
    )

    console.print(Panel(table))


@app.command()
def retry(
    data_dir: Path = typer.Option("data", help="Data directory path"),
):
    """Retry failed download tasks."""
    retry_manager = RetryManager()
    failed_tasks = retry_manager.load()

    if not failed_tasks:
        console.print("[yellow]No failed tasks to retry")
        return

    store = AggTradesStore(str(data_dir))
    new_failed_tasks = []

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TimeRemainingColumn(),
    )

    with Live(progress):
        task_id = progress.add_task("Retrying failed tasks", total=len(failed_tasks))

        for task in failed_tasks:
            try:
                symbol = task["symbol"]
                date = dt.date.fromisoformat(task["date"])

                hour = task.get("hour", 0)  # Default to hour 0 for old retry files
                df = get_hourly_agg_trades(symbol, date, hour)
                if not df.empty:
                    store.write_trades(symbol, df)
                    console.print(
                        f"[green]Successfully retried {symbol} for {date} hour {hour}"
                    )
            except Exception as e:
                console.print(f"[red]Retry failed for {task}: {e}")
                new_failed_tasks.append(task)

            progress.advance(task_id)

    if new_failed_tasks:
        retry_manager.save(new_failed_tasks)
        console.print("[yellow]Some tasks still failed. Retry file updated")
    else:
        retry_manager.clear()
        console.print("[green]All retries completed successfully")


if __name__ == "__main__":
    app()
