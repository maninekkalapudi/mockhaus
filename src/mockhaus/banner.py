"""ASCII banner and branding for Mockhaus."""

import sys
from typing import Optional

try:
    from colorama import Fore, Style, init
    init(autoreset=True)
    HAS_COLOR = True
except ImportError:
    HAS_COLOR = False

try:
    import duckdb
    DUCKDB_VERSION = duckdb.__version__
except ImportError:
    DUCKDB_VERSION = "Unknown"


BANNER = r"""
╔══════════════════════════════════════════════════════════════════════════╗
║                                                                          ║
║   ███╗   ███╗ ██████╗  ██████╗██╗  ██╗██╗  ██╗ █████╗ ██╗   ██╗███████╗  ║
║   ████╗ ████║██╔═══██╗██╔════╝██║ ██╔╝██║  ██║██╔══██╗██║   ██║██╔════╝  ║
║   ██╔████╔██║██║   ██║██║     █████╔╝ ███████║███████║██║   ██║███████╗  ║
║   ██║╚██╔╝██║██║   ██║██║     ██╔═██╗ ██╔══██║██╔══██║██║   ██║╚════██║  ║
║   ██║ ╚═╝ ██║╚██████╔╝╚██████╗██║  ██╗██║  ██║██║  ██║╚██████╔╝███████║  ║
║   ╚═╝     ╚═╝ ╚═════╝  ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚══════╝  ║
║                                                                          ║
║               Snowflake-Compatible SQL Engine                            ║
║               Powered by DuckDB v{duckdb_version:<20}                    ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

SIMPLE_BANNER = """
==============================================================================
                            M O C K H A U S
------------------------------------------------------------------------------
            Snowflake-Compatible SQL Engine | DuckDB v{duckdb_version}
==============================================================================
"""

REPL_WELCOME = """
Type 'help' for commands, 'exit' or Ctrl+D to quit.
Connected to in-memory database. Use ':memory:' or provide a path to persist.
"""

SERVER_WELCOME = """
Server ready! API documentation available at /docs
"""


def get_colored_banner(banner_text: str, color: Optional[str] = None) -> str:
    """Apply color to banner if colorama is available."""
    if not HAS_COLOR or not color:
        return banner_text

    color_map = {
        'cyan': Fore.CYAN,
        'green': Fore.GREEN,
        'yellow': Fore.YELLOW,
        'blue': Fore.BLUE,
        'magenta': Fore.MAGENTA,
        'red': Fore.RED,
        'white': Fore.WHITE,
    }

    fore_color = color_map.get(color.lower(), Fore.CYAN)
    return f"{fore_color}{banner_text}{Style.RESET_ALL}"


def print_banner(mode: str = 'full', color: Optional[str] = 'cyan') -> None:
    """
    Print the Mockhaus banner.

    Args:
        mode: 'full' for detailed ASCII art, 'simple' for basic text
        color: Color name ('cyan', 'green', 'yellow', 'blue', 'magenta', 'red', 'white')
    """
    if mode == 'full':
        banner_text = BANNER.format(duckdb_version=DUCKDB_VERSION)
    else:
        banner_text = SIMPLE_BANNER.format(duckdb_version=DUCKDB_VERSION)

    colored_banner = get_colored_banner(banner_text, color)
    print(colored_banner, file=sys.stderr)


def print_repl_banner(color: Optional[str] = 'cyan') -> None:
    """Print banner for REPL mode."""
    print_banner('full', color)
    print(get_colored_banner(REPL_WELCOME, 'green'), file=sys.stderr)


def print_server_banner(host: str = '0.0.0.0', port: int = 8080, color: Optional[str] = 'cyan') -> None:
    """Print banner for server mode."""
    print_banner('full', color)
    server_info = f"Starting server on http://{host}:{port}"
    print(get_colored_banner(server_info, 'green'), file=sys.stderr)
    print(get_colored_banner(SERVER_WELCOME, 'green'), file=sys.stderr)


if __name__ == "__main__":
    # Test the banners
    print("Full banner:")
    print_banner('full')
    print("\nSimple banner:")
    print_banner('simple')
    print("\nREPL banner:")
    print_repl_banner()
    print("\nServer banner:")
    print_server_banner()
