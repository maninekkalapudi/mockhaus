"""
This module defines the command-line interface (CLI) for Mockhaus.

It uses the `click` library to create a rich set of commands for interacting
with the Mockhaus engine, including executing queries, managing the server,
and handling data ingestion objects like stages and file formats.
"""

from datetime import UTC, datetime, timedelta

import click
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

from .query_history import QueryHistory

# Initialize Rich console for pretty output
console = Console()


@click.group()
@click.version_option()
def main() -> None:
    """Mockhaus - Snowflake proxy with DuckDB backend."""
    pass


@main.command()
@click.option("--host", default="localhost", help="Host to bind server")
@click.option("--port", default=8080, type=int, help="Port to bind server")
@click.option("--database", "-d", default=None, help="Database file (ignored in server mode)")
@click.option("--daemon", is_flag=True, help="Run as daemon")
def serve(host: str, port: int, database: str | None, daemon: bool) -> None:  # noqa: ARG001
    """Start Mockhaus HTTP server."""
    try:
        import uvicorn
    except ImportError:
        console.print("[red]Error: uvicorn not installed. Run: uv sync[/red]")
        return

    console.print(f"[green]Starting Mockhaus server at http://{host}:{port}[/green]")
    console.print("[cyan]🔗 Session-based architecture - supports multiple concurrent users[/cyan]")
    console.print("[dim]• Memory sessions: Data isolated per session, lost when session ends[/dim]")
    console.print("[dim]• Persistent sessions: Data saved to disk, survives server restarts[/dim]")
    console.print("[dim]• Query history: Per-session, in-memory only (not persisted)[/dim]")
    console.print(f"[dim]• API documentation available at http://{host}:{port}/docs[/dim]")
    console.print("[dim]Press Ctrl+C to stop the server[/dim]\n")

    try:
        uvicorn.run("mockhaus.server.app:app", host=host, port=port, reload=not daemon, log_level="info" if not daemon else "warning")
    except KeyboardInterrupt:
        console.print("\n[yellow]Server stopped[/yellow]")


@main.command()
@click.option("--session-type", default="memory", type=click.Choice(["memory", "persistent"]), help="Session type")
@click.option("--session-id", help="Specific session ID to use")
@click.option("--session-ttl", type=int, help="Session TTL in seconds")
@click.option("--persistent-path", help="Path for persistent session storage")
def repl(session_type: str, session_id: str | None, session_ttl: int | None, persistent_path: str | None) -> None:
    """Start interactive REPL client."""
    try:
        # Import the enhanced REPL directly
        from .repl.enhanced_repl import main as enhanced_repl_main

        enhanced_repl_main(session_type=session_type, session_id=session_id, session_ttl=session_ttl, persistent_path=persistent_path)
    except ImportError as e:
        console.print(f"[red]Error: Enhanced REPL module not found: {e}[/red]")
        console.print("[dim]Make sure all dependencies are installed with: uv sync[/dim]")
    except KeyboardInterrupt:
        console.print("\n[yellow]REPL interrupted[/yellow]")


# Create a command group for stage management
@main.group()
def history() -> None:
    """Manage query history."""
    pass


@history.command("recent")
@click.option("--limit", "-n", default=10, help="Number of queries to show")
@click.option("--verbose", "-v", is_flag=True, help="Show full query text")
def history_recent(limit: int, verbose: bool) -> None:
    """Show recent query history."""
    history = QueryHistory()

    try:
        records = history.get_recent(limit=limit)

        if not records:
            console.print("[yellow]No query history found[/yellow]")
            return

        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("ID", style="dim", width=8)
        table.add_column("Timestamp", style="dim")
        table.add_column("Status", justify="center")
        table.add_column("Type", style="cyan")
        table.add_column("Query", no_wrap=not verbose)
        table.add_column("Time (ms)", justify="right")

        for record in records:
            # Format status with color
            status = "[green]✓[/green]" if record.status == "SUCCESS" else "[red]✗[/red]"

            # Truncate query if not verbose
            query_text = record.original_sql
            if not verbose and len(query_text) > 50:
                query_text = query_text[:50] + "..."

            table.add_row(
                str(record.id),
                record.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                status,
                record.query_type or "?",
                query_text,
                str(record.execution_time_ms) if record.execution_time_ms else "-",
            )

        console.print(table)
        console.print(f"\n[dim]Showing {len(records)} most recent queries[/dim]")

    except Exception as e:
        console.print(f"[red]Error reading history: {e}[/red]")
    finally:
        history.close()


@history.command("search")
@click.option("--text", "-t", help="Search text in queries")
@click.option("--status", "-s", type=click.Choice(["SUCCESS", "ERROR"]), help="Filter by status")
@click.option("--type", "-y", help="Filter by query type (SELECT, INSERT, etc)")
@click.option("--days", "-d", default=7, help="Search last N days")
@click.option("--limit", "-n", default=20, help="Maximum results to show")
def history_search(text: str, status: str, type: str, days: int, limit: int) -> None:
    """Search query history."""
    history = QueryHistory()

    try:
        # Calculate start time
        start_time = datetime.now(UTC) - timedelta(days=days)

        records = history.search(text=text, status=status, query_type=type, start_time=start_time, limit=limit)

        if not records:
            console.print("[yellow]No matching queries found[/yellow]")
            return

        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("ID", style="dim", width=8)
        table.add_column("Timestamp", style="dim")
        table.add_column("Status", justify="center")
        table.add_column("Type", style="cyan")
        table.add_column("Query", no_wrap=False)
        table.add_column("Time (ms)", justify="right")

        for record in records:
            # Format status with color
            status_display = "[green]✓[/green]" if record.status == "SUCCESS" else "[red]✗[/red]"

            table.add_row(
                str(record.id),
                record.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                status_display,
                record.query_type or "?",
                record.original_sql[:100] + "..." if len(record.original_sql) > 100 else record.original_sql,
                str(record.execution_time_ms) if record.execution_time_ms else "-",
            )

        console.print(table)
        console.print(f"\n[dim]Found {len(records)} matching queries[/dim]")

    except Exception as e:
        console.print(f"[red]Error searching history: {e}[/red]")
    finally:
        history.close()


@history.command("show")
@click.argument("query_id")
def history_show(query_id: str) -> None:
    """Show details of a specific query."""
    history = QueryHistory()

    try:
        record = history.get_by_id(query_id)

        if not record:
            console.print(f"[red]Query with ID '{query_id}' not found[/red]")
            return

        # Create info panel
        info_text = Text()
        info_text.append("Query ID: ", style="bold")
        info_text.append(f"{record.query_id}\n")
        info_text.append("Timestamp: ", style="bold")
        info_text.append(f"{record.timestamp}\n")
        info_text.append("Status: ", style="bold")
        if record.status == "SUCCESS":
            info_text.append("SUCCESS", style="green")
        else:
            info_text.append("ERROR", style="red")
        info_text.append("\n")
        info_text.append("Type: ", style="bold")
        info_text.append(f"{record.query_type or 'Unknown'}\n")
        info_text.append("Execution Time: ", style="bold")
        info_text.append(f"{record.execution_time_ms}ms\n")

        if record.rows_affected is not None:
            info_text.append("Rows Affected: ", style="bold")
            info_text.append(f"{record.rows_affected}\n")

        if record.database_name:
            info_text.append("Database: ", style="bold")
            info_text.append(f"{record.database_name}\n")

        if record.error_message:
            info_text.append("\nError: ", style="bold red")
            info_text.append(f"{record.error_message}\n", style="red")

        console.print(Panel(info_text, title="Query Details", border_style="blue"))

        # Show original SQL
        console.print("\n[bold]Original SQL:[/bold]")
        syntax = Syntax(record.original_sql, "sql", theme="monokai", line_numbers=True)
        console.print(Panel(syntax, border_style="yellow"))

        # Show translated SQL if different
        if record.translated_sql and record.translated_sql != record.original_sql:
            console.print("\n[bold]Translated SQL:[/bold]")
            syntax = Syntax(record.translated_sql, "sql", theme="monokai", line_numbers=True)
            console.print(Panel(syntax, border_style="green"))

    except Exception as e:
        console.print(f"[red]Error showing query: {e}[/red]")
    finally:
        history.close()


@history.command("stats")
@click.option("--days", "-d", default=1, help="Show stats for last N days")
def history_stats(days: int) -> None:
    """Show query statistics."""
    history = QueryHistory()

    try:
        start_time = datetime.now(UTC) - timedelta(days=days)
        end_time = datetime.now(UTC)

        stats = history.get_statistics(start_time, end_time)

        # Overview panel
        overview_text = Text()
        overview_text.append("Total Queries: ", style="bold")
        overview_text.append(f"{stats.total_queries}\n")
        overview_text.append("Successful: ", style="bold")
        overview_text.append(f"{stats.successful_queries}", style="green")
        overview_text.append(f" ({stats.successful_queries / stats.total_queries * 100:.1f}%)\n" if stats.total_queries > 0 else " (0%)\n")
        overview_text.append("Failed: ", style="bold")
        overview_text.append(f"{stats.failed_queries}", style="red")
        overview_text.append(f" ({stats.failed_queries / stats.total_queries * 100:.1f}%)\n" if stats.total_queries > 0 else " (0%)\n")
        overview_text.append("\nAvg Execution Time: ", style="bold")
        overview_text.append(f"{stats.avg_execution_time_ms:.2f}ms\n")
        overview_text.append("95th Percentile: ", style="bold")
        overview_text.append(f"{stats.p95_execution_time_ms:.2f}ms\n")

        console.print(Panel(overview_text, title=f"Query Statistics (Last {days} day{'s' if days != 1 else ''})", border_style="cyan"))

        # Query types breakdown
        if stats.queries_by_type:
            console.print("\n[bold]Queries by Type:[/bold]")
            type_table = Table(show_header=True, header_style="bold cyan")
            type_table.add_column("Type", style="cyan")
            type_table.add_column("Count", justify="right")
            type_table.add_column("Percentage", justify="right")

            for query_type, count in sorted(stats.queries_by_type.items(), key=lambda x: x[1], reverse=True):
                percentage = count / stats.total_queries * 100 if stats.total_queries > 0 else 0
                type_table.add_row(query_type, str(count), f"{percentage:.1f}%")

            console.print(type_table)

        # Error breakdown
        if stats.errors_by_code:
            console.print("\n[bold]Errors by Type:[/bold]")
            error_table = Table(show_header=True, header_style="bold red")
            error_table.add_column("Error Type", style="red")
            error_table.add_column("Count", justify="right")

            for error_code, count in sorted(stats.errors_by_code.items(), key=lambda x: x[1], reverse=True):
                error_table.add_row(error_code, str(count))

            console.print(error_table)

    except Exception as e:
        console.print(f"[red]Error getting statistics: {e}[/red]")
    finally:
        history.close()


@history.command("clear")
@click.option("--before", "-b", help="Clear queries before this date (YYYY-MM-DD)")
@click.option("--force", "-f", is_flag=True, help="Skip confirmation")
def history_clear(before: str, force: bool) -> None:
    """Clear query history."""
    history = QueryHistory()

    try:
        before_date = None
        if before:
            try:
                before_date = datetime.strptime(before, "%Y-%m-%d").replace(tzinfo=UTC)
            except ValueError:
                console.print("[red]Invalid date format. Use YYYY-MM-DD[/red]")
                return

        # Confirmation
        if not force:
            if before_date:
                confirm = click.confirm(f"Clear all queries before {before}?")
            else:
                confirm = click.confirm("Clear ALL query history? This cannot be undone!")

            if not confirm:
                console.print("[yellow]Cancelled[/yellow]")
                return

        # Clear history
        count = history.clear_history(before_date)

        if before_date:
            console.print(f"[green]✓ Cleared {count} queries before {before}[/green]")
        else:
            console.print(f"[green]✓ Cleared all {count} queries from history[/green]")

    except Exception as e:
        console.print(f"[red]Error clearing history: {e}[/red]")
    finally:
        history.close()


@history.command("export")
@click.option("--format", "-f", type=click.Choice(["json", "csv"]), default="json", help="Export format")
@click.option("--output", "-o", required=True, help="Output file path")
@click.option("--days", "-d", default=7, help="Export last N days")
def history_export(format: str, output: str, days: int) -> None:
    """Export query history."""
    history = QueryHistory()

    try:
        if format == "json":
            filters = {"start_time": datetime.now(UTC) - timedelta(days=days)}
            history.export_json(output, filters)
            console.print(f"[green]✓ Exported query history to {output}[/green]")
        elif format == "csv":
            history.export_csv(output)
            console.print(f"[green]✓ Exported query history to {output}[/green]")

    except Exception as e:
        console.print(f"[red]Error exporting history: {e}[/red]")
    finally:
        history.close()


if __name__ == "__main__":
    main()
