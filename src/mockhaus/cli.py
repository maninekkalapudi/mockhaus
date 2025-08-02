"""Command line interface for Mockhaus."""

from datetime import UTC, datetime, timedelta
from typing import Any

import click
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

from .executor import MockhausExecutor
from .query_history import QueryHistory

console = Console()


@click.group()
@click.version_option()
def main() -> None:
    """Mockhaus - Snowflake proxy with DuckDB backend."""
    pass


@main.command()
@click.argument("sql", required=False)
@click.option("--file", "-f", type=click.File("r"), help="Read SQL from file")
@click.option("--database", "-d", default=None, help="DuckDB database file (default: in-memory)")
@click.option("--verbose", "-v", is_flag=True, help="Show translation details")
def query(sql: Any, file: Any, database: Any, verbose: Any) -> None:
    """Execute a Snowflake SQL query."""

    # Get SQL from argument or file
    if file:
        sql = file.read()
    elif not sql:
        console.print("[red]Error: Provide SQL query as argument or use --file option[/red]")
        return

    # Execute the query
    with MockhausExecutor(database) as executor:
        # Create sample data if using in-memory database
        if database is None:
            executor.create_sample_data()
            console.print("[dim]Created sample data in in-memory database[/dim]\n")

        result = executor.execute_snowflake_sql(sql)

        # Display results
        if result.success:
            _display_successful_result(result, verbose)
        else:
            _display_error_result(result, verbose)


@main.command()
@click.option("--host", default="localhost", help="Host to bind server")
@click.option("--port", default=8080, type=int, help="Port to bind server")
@click.option("--database", "-d", default=None, help="Default database file")
@click.option("--daemon", is_flag=True, help="Run as daemon")
def serve(host: str, port: int, database: str, daemon: bool) -> None:
    """Start Mockhaus HTTP server."""
    try:
        import uvicorn
    except ImportError:
        console.print("[red]Error: uvicorn not installed. Run: uv sync[/red]")
        return

    console.print(f"[green]Starting Mockhaus server at http://{host}:{port}[/green]")

    if database:
        console.print(f"[dim]Default database: {database}[/dim]")
    else:
        console.print("[dim]Using in-memory database (with sample data)[/dim]")

    console.print(f"[dim]API documentation available at http://{host}:{port}/docs[/dim]")
    console.print("[dim]Press Ctrl+C to stop the server[/dim]\n")

    # Set environment variable for default database if specified
    if database:
        import os

        os.environ["MOCKHAUS_DEFAULT_DATABASE"] = database

    try:
        uvicorn.run("mockhaus.server.app:app", host=host, port=port, reload=not daemon, log_level="info" if not daemon else "warning")
    except KeyboardInterrupt:
        console.print("\n[yellow]Server stopped[/yellow]")


@main.command()
def sample() -> None:
    """Show sample queries that work with Mockhaus."""

    samples = [
        {
            "description": "Basic SELECT",
            "sql": "SELECT customer_id, customer_name FROM sample_customers",
        },
        {
            "description": "SELECT with WHERE clause",
            "sql": "SELECT customer_name, account_balance FROM sample_customers WHERE account_balance > 1000",
        },
        {
            "description": "SELECT with ORDER BY",
            "sql": "SELECT customer_name, signup_date FROM sample_customers ORDER BY signup_date DESC",
        },
        {
            "description": "Aggregate query",
            "sql": "SELECT COUNT(*) as total_customers, AVG(account_balance) as avg_balance FROM sample_customers",
        },
        {
            "description": "Date functions",
            "sql": "SELECT customer_name, signup_date, CURRENT_DATE as today FROM sample_customers",
        },
    ]

    console.print("[bold]Sample Queries for Mockhaus[/bold]\n")

    for i, sample in enumerate(samples, 1):
        console.print(f"[bold cyan]{i}. {sample['description']}[/bold cyan]")
        syntax = Syntax(sample["sql"], "sql", theme="monokai", line_numbers=False)
        console.print(Panel(syntax, border_style="blue"))
        console.print()


@main.command()
def repl() -> None:
    """Start interactive REPL client."""
    try:
        # Import the repl module which will automatically select the best REPL
        from .repl import repl_main

        repl_main()
    except ImportError:
        console.print("[red]Error: REPL module not found. Make sure you're running from the project root.[/red]")
        console.print("[dim]Try: python -m mockhaus.repl.enhanced_repl[/dim]")
    except KeyboardInterrupt:
        console.print("\n[yellow]REPL interrupted[/yellow]")


@main.command()
@click.option("--database", "-d", default=None, help="DuckDB database file (default: in-memory)")
def setup(database: Any) -> None:
    """Set up sample data for testing."""

    with MockhausExecutor(database) as executor:
        executor.create_sample_data()

        # Test that it worked
        result = executor.execute_snowflake_sql("SELECT COUNT(*) as count FROM sample_customers")

        if result.success and result.data:
            count = result.data[0]["count"]
            console.print(f"[green]✓ Successfully created sample data with {count} records[/green]")

            if database:
                console.print(f"[dim]Data saved to: {database}[/dim]")
            else:
                console.print("[dim]Data created in memory (will be lost when program exits)[/dim]")
        else:
            console.print(f"[red]✗ Failed to create sample data: {result.error}[/red]")


@main.group()
def stage() -> None:
    """Manage Snowflake stages."""
    pass


@stage.command("list")
@click.option("--database", "-d", default=None, help="DuckDB database file (default: in-memory)")
def list_stages(database: Any) -> None:
    """List all stages."""
    with MockhausExecutor(database) as executor:
        try:
            executor.connect()  # Ensure connection is established
            if not executor._ingestion_handler:
                console.print("[red]Error: Ingestion handler not initialized[/red]")
                return
            stages = executor._ingestion_handler.stage_manager.list_stages()

            if not stages:
                console.print("[yellow]No stages found[/yellow]")
                return

            table = Table(show_header=True, header_style="bold cyan")
            table.add_column("Name", style="bold")
            table.add_column("Type")
            table.add_column("URL")
            table.add_column("Local Path", style="dim")
            table.add_column("Created", style="dim")

            for stage in stages:
                table.add_row(
                    stage.name,
                    stage.stage_type,
                    stage.url or "[dim]N/A[/dim]",
                    stage.local_path,
                    str(stage.created_at) if stage.created_at else "[dim]N/A[/dim]",
                )

            console.print(table)
            console.print(f"\n[dim]{len(stages)} stage(s) found[/dim]")

        except Exception as e:
            console.print(f"[red]Error listing stages: {e}[/red]")


@stage.command("show")
@click.argument("name")
@click.option("--database", "-d", default=None, help="DuckDB database file (default: in-memory)")
def show_stage(name: str, database: Any) -> None:
    """Show detailed information about a stage."""
    with MockhausExecutor(database) as executor:
        try:
            executor.connect()  # Ensure connection is established
            if not executor._ingestion_handler:
                console.print("[red]Error: Ingestion handler not initialized[/red]")
                return
            stage = executor._ingestion_handler.stage_manager.get_stage(name)

            if not stage:
                console.print(f"[red]Stage '{name}' not found[/red]")
                return

            # Create info panel
            info_text = Text()
            info_text.append("Name: ", style="bold")
            info_text.append(f"{stage.name}\n")
            info_text.append("Type: ", style="bold")
            info_text.append(f"{stage.stage_type}\n")
            info_text.append("URL: ", style="bold")
            info_text.append(f"{stage.url or 'N/A'}\n")
            info_text.append("Local Path: ", style="bold")
            info_text.append(f"{stage.local_path}\n")
            info_text.append("Created: ", style="bold")
            info_text.append(f"{str(stage.created_at) if stage.created_at else 'N/A'}\n")

            if stage.properties:
                info_text.append("\nProperties:\n", style="bold")
                for key, value in stage.properties.items():
                    info_text.append(f"  {key}: {value}\n", style="dim")

            console.print(Panel(info_text, title=f"Stage: {name}", border_style="blue"))

            # List files in stage
            try:
                files = executor._ingestion_handler.stage_manager.list_stage_files(f"@{name}")
                if files:
                    console.print(f"\n[bold]Files in stage ({len(files)}):[/bold]")
                    for file in files[:10]:  # Show first 10 files
                        console.print(f"  [dim]•[/dim] {file}")
                    if len(files) > 10:
                        console.print(f"  [dim]... and {len(files) - 10} more files[/dim]")
                else:
                    console.print("\n[dim]No files found in stage[/dim]")
            except Exception:
                console.print("\n[dim]Could not list files in stage[/dim]")

        except Exception as e:
            console.print(f"[red]Error showing stage: {e}[/red]")


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


@main.group()
def format() -> None:
    """Manage file formats."""
    pass


@format.command("list")
@click.option("--database", "-d", default=None, help="DuckDB database file (default: in-memory)")
def list_formats(database: Any) -> None:
    """List all file formats."""
    with MockhausExecutor(database) as executor:
        try:
            executor.connect()  # Ensure connection is established
            if not executor._ingestion_handler:
                console.print("[red]Error: Ingestion handler not initialized[/red]")
                return
            formats = executor._ingestion_handler.format_manager.list_formats()

            if not formats:
                console.print("[yellow]No file formats found[/yellow]")
                return

            table = Table(show_header=True, header_style="bold cyan")
            table.add_column("Name", style="bold")
            table.add_column("Type")
            table.add_column("Key Properties", style="dim")
            table.add_column("Created", style="dim")

            for fmt in formats:
                # Show key properties as a summary
                key_props = []
                if fmt.format_type == "CSV":
                    delimiter = fmt.properties.get("field_delimiter", ",")
                    skip_header = fmt.properties.get("skip_header", 0)
                    key_props.append(f"delimiter='{delimiter}'")
                    if skip_header:
                        key_props.append(f"skip_header={skip_header}")
                elif fmt.format_type in ["JSON", "PARQUET"]:
                    compression = fmt.properties.get("compression", "AUTO")
                    if compression != "AUTO":
                        key_props.append(f"compression={compression}")

                key_props_str = ", ".join(key_props) if key_props else "defaults"

                table.add_row(fmt.name, fmt.format_type, key_props_str, str(fmt.created_at) if fmt.created_at else "[dim]N/A[/dim]")

            console.print(table)
            console.print(f"\n[dim]{len(formats)} format(s) found[/dim]")

        except Exception as e:
            console.print(f"[red]Error listing formats: {e}[/red]")


@format.command("show")
@click.argument("name")
@click.option("--database", "-d", default=None, help="DuckDB database file (default: in-memory)")
def show_format(name: str, database: Any) -> None:
    """Show detailed information about a file format."""
    with MockhausExecutor(database) as executor:
        try:
            executor.connect()  # Ensure connection is established
            if not executor._ingestion_handler:
                console.print("[red]Error: Ingestion handler not initialized[/red]")
                return
            fmt = executor._ingestion_handler.format_manager.get_format(name)

            if not fmt:
                console.print(f"[red]File format '{name}' not found[/red]")
                return

            # Create info panel
            info_text = Text()
            info_text.append("Name: ", style="bold")
            info_text.append(f"{fmt.name}\n")
            info_text.append("Type: ", style="bold")
            info_text.append(f"{fmt.format_type}\n")
            info_text.append("Created: ", style="bold")
            info_text.append(f"{str(fmt.created_at) if fmt.created_at else 'N/A'}\n")

            if fmt.properties:
                info_text.append("\nProperties:\n", style="bold")
                for key, value in sorted(fmt.properties.items()):
                    info_text.append(f"  {key}: ", style="cyan")
                    info_text.append(f"{value}\n")

            console.print(Panel(info_text, title=f"File Format: {name}", border_style="green"))

            # Show DuckDB mapping
            try:
                duck_options = executor._ingestion_handler.format_manager.map_to_duckdb_options(fmt)
                if duck_options:
                    console.print("\n[bold]DuckDB Options:[/bold]")
                    for key, value in duck_options.items():
                        console.print(f"  [cyan]{key}[/cyan]: {value}")
            except Exception:
                console.print("\n[dim]Could not generate DuckDB options[/dim]")

        except Exception as e:
            console.print(f"[red]Error showing format: {e}[/red]")


def _display_successful_result(result: Any, verbose: Any) -> None:
    """Display a successful query result."""

    # Show translation info if verbose
    if verbose:
        console.print("[bold]Translation Details[/bold]")

        console.print("[dim]Original Snowflake SQL:[/dim]")
        original_syntax = Syntax(result.original_sql, "sql", theme="monokai", line_numbers=False)
        console.print(Panel(original_syntax, border_style="yellow"))

        console.print("[dim]Translated DuckDB SQL:[/dim]")
        translated_syntax = Syntax(result.translated_sql, "sql", theme="monokai", line_numbers=False)
        console.print(Panel(translated_syntax, border_style="green"))

        console.print(f"[dim]Execution time: {result.execution_time_ms:.2f}ms[/dim]\n")

    # Show results
    if result.data:
        table = Table(show_header=True, header_style="bold magenta")

        # Add columns
        if result.columns:
            for column in result.columns:
                table.add_column(column)

        # Add rows
        for row in result.data:
            row_values = []
            for column in result.columns or []:
                value = row.get(column, "")
                row_values.append(str(value))
            table.add_row(*row_values)

        console.print(table)
        console.print(f"\n[dim]{result.row_count} row(s) returned in {result.execution_time_ms:.2f}ms[/dim]")
    else:
        console.print("[yellow]Query executed successfully but returned no data[/yellow]")
        console.print(f"[dim]Execution time: {result.execution_time_ms:.2f}ms[/dim]")


def _display_error_result(result: Any, verbose: Any) -> None:
    """Display an error result."""

    console.print("[bold red]Query Failed[/bold red]")
    console.print(f"[red]Error: {result.error}[/red]")

    if verbose and result.original_sql:
        console.print("\n[dim]Original SQL:[/dim]")
        original_syntax = Syntax(result.original_sql, "sql", theme="monokai", line_numbers=False)
        console.print(Panel(original_syntax, border_style="red"))

    console.print(f"[dim]Execution time: {result.execution_time_ms:.2f}ms[/dim]")


if __name__ == "__main__":
    main()
