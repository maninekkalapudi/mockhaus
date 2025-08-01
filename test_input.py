#!/usr/bin/env python3
"""Quick test for enhanced REPL input functionality."""

import sys
import os

# Add the current directory to path
sys.path.insert(0, os.path.abspath('.'))

def test_input_functionality():
    """Test that the input system works properly."""
    print("Testing enhanced REPL input system...")
    
    try:
        from client.enhanced_repl import EnhancedMockhausClient, PROMPT_TOOLKIT_AVAILABLE
        
        if not PROMPT_TOOLKIT_AVAILABLE:
            print("❌ prompt_toolkit not available - cannot test enhanced features")
            return False
            
        client = EnhancedMockhausClient()
        print("✅ Enhanced client created successfully")
        
        # Test that the get_input method exists and is callable
        if hasattr(client, 'get_input') and callable(client.get_input):
            print("✅ get_input method is available")
        else:
            print("❌ get_input method not found")
            return False
            
        print("✅ Input system appears to be working correctly")
        print("\nTo test interactively:")
        print("1. Start server: uv run mockhaus serve")
        print("2. In another terminal: uv run python -m client.enhanced_repl")
        print("3. Try commands like 'health', 'help', or SQL queries")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_input_functionality()
    sys.exit(0 if success else 1)