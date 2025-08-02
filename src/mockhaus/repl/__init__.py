"""Mockhaus client package with automatic REPL selection."""

# Automatic fallback system - prefer enhanced REPL if available
try:
    from .enhanced_repl import EnhancedMockhausClient as MockhausClient
    from .enhanced_repl import main as repl_main

    print("🚀 Enhanced REPL loaded (prompt_toolkit available)")
    __repl_type__ = "enhanced"
except ImportError:
    from .repl import MockhausClient
    from .repl import main as repl_main

    print("📱 Basic REPL loaded (prompt_toolkit not available)")
    print("   For enhanced features, install: uv add prompt-toolkit")
    __repl_type__ = "basic"

__all__ = ["MockhausClient", "repl_main"]
