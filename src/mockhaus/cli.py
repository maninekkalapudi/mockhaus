"""Command line interface for Mockhaus."""

from typing import Any

import click
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from .executor import MockhausExecutor

console = Console()


@click.group()
@click.version_option()
def main() -> None:
    """Mockhaus - Snowflake proxy with DuckDB backend."""
    pass


@main.command()
@click.argument("sql", required=False)
@click.option("--file", "-f", type=click.File("r"), help="Read SQL from file")
@click.option(
    "--database", "-d", default=None, help="DuckDB database file (default: in-memory)"
)
@click.option("--verbose", "-v", is_flag=True, help="Show translation details")
def query(sql: Any, file: Any, database: Any, verbose: Any) -> None:
    """Execute a Snowflake SQL query."""

    # Get SQL from argument or file
    if file:
        sql = file.read()
    elif not sql:
        console.print(
            "[red]Error: Provide SQL query as argument or use --file option[/red]"
        )
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
@click.option(
    "--database", "-d", default=None, help="DuckDB database file (default: in-memory)"
)
def setup(database: Any) -> None:
    """Set up sample data for testing."""

    with MockhausExecutor(database) as executor:
        executor.create_sample_data()

        # Test that it worked
        result = executor.execute_snowflake_sql(
            "SELECT COUNT(*) as count FROM sample_customers"
        )

        if result.success and result.data:
            count = result.data[0]["count"]
            console.print(
                f"[green]✓ Successfully created sample data with {count} records[/green]"
            )

            if database:
                console.print(f"[dim]Data saved to: {database}[/dim]")
            else:
                console.print(
                    "[dim]Data created in memory (will be lost when program exits)[/dim]"
                )
        else:
            console.print(f"[red]✗ Failed to create sample data: {result.error}[/red]")


def _display_successful_result(result: Any, verbose: Any) -> None:
    """Display a successful query result."""

    # Show translation info if verbose
    if verbose:
        console.print("[bold]Translation Details[/bold]")

        console.print("[dim]Original Snowflake SQL:[/dim]")
        original_syntax = Syntax(
            result.original_sql, "sql", theme="monokai", line_numbers=False
        )
        console.print(Panel(original_syntax, border_style="yellow"))

        console.print("[dim]Translated DuckDB SQL:[/dim]")
        translated_syntax = Syntax(
            result.translated_sql, "sql", theme="monokai", line_numbers=False
        )
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
        console.print(
            f"\n[dim]{result.row_count} row(s) returned in {result.execution_time_ms:.2f}ms[/dim]"
        )
    else:
        console.print(
            "[yellow]Query executed successfully but returned no data[/yellow]"
        )
        console.print(f"[dim]Execution time: {result.execution_time_ms:.2f}ms[/dim]")


def _display_error_result(result: Any, verbose: Any) -> None:
    """Display an error result."""

    console.print("[bold red]Query Failed[/bold red]")
    console.print(f"[red]Error: {result.error}[/red]")

    if verbose and result.original_sql:
        console.print("\n[dim]Original SQL:[/dim]")
        original_syntax = Syntax(
            result.original_sql, "sql", theme="monokai", line_numbers=False
        )
        console.print(Panel(original_syntax, border_style="red"))

    console.print(f"[dim]Execution time: {result.execution_time_ms:.2f}ms[/dim]")


if __name__ == "__main__":
    main()
