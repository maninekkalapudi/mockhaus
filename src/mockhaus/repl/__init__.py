"""Mockhaus client package with automatic REPL selection."""

# Automatic fallback system - prefer enhanced REPL if available
try:
    from .enhanced_repl import EnhancedMockhausClient as MockhausClient
    from .enhanced_repl import main as repl_main

    __repl_type__ = "enhanced"
except ImportError:
    from .repl import MockhausClient  # type: ignore[assignment]
    from .repl import main as repl_main

    __repl_type__ = "basic"

__all__ = ["MockhausClient", "repl_main"]
