"""
This module defines the ASCII art banners and welcome messages for Mockhaus.

It provides functions to print styled and colored banners for different
application modes (e.g., REPL, server). It uses `colorama` for cross-platform
colored output, with a graceful fallback if the library is not installed.
"""

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
    """
    Applies color to the banner text if colorama is available.

    Args:
        banner_text: The text of the banner.
        color: The desired color name (e.g., 'cyan', 'green').

    Returns:
        The colored banner text, or the original text if color is not supported.
    """
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
    Prints the Mockhaus banner to stderr.

    Args:
        mode: The banner style to use ('full' for ASCII art, 'simple' for basic text).
        color: The color to apply to the banner.
    """
    if mode == 'full':
        banner_text = BANNER.format(duckdb_version=DUCKDB_VERSION)
    else:
        banner_text = SIMPLE_BANNER.format(duckdb_version=DUCKDB_VERSION)

    colored_banner = get_colored_banner(banner_text, color)
    print(colored_banner, file=sys.stderr)


def print_repl_banner(color: Optional[str] = 'cyan') -> None:
    """
    Prints the specific banner and welcome message for REPL mode.

    Args:
        color: The color to apply to the banner.
    """
    print_banner('full', color)
    print(get_colored_banner(REPL_WELCOME, 'green'), file=sys.stderr)


def print_server_banner(host: str = '0.0.0.0', port: int = 8080, color: Optional[str] = 'cyan') -> None:
    """
    Prints the specific banner and welcome message for server mode.

    Args:
        host: The host the server is running on.
        port: The port the server is running on.
        color: The color to apply to the banner.
    """
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