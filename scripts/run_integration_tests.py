#!/usr/bin/env python3
"""Script to run integration tests with proper setup and teardown."""

import subprocess
import sys
import os
from pathlib import Path

def run_integration_tests():
    """Run integration tests with proper environment setup."""
    # Get the project root directory
    project_root = Path(__file__).parent.parent
    
    # Set environment variables for testing
    env = os.environ.copy()
    env["MOCKHAUS_ENV"] = "test"
    env["PYTHONPATH"] = str(project_root / "src")
    
    # pytest command
    cmd = [
        sys.executable, "-m", "pytest",
        str(project_root / "tests" / "integration"),
        "-v",  # Verbose output
        "--tb=short",  # Short traceback format
        "-x",  # Stop on first failure
        "--color=yes",  # Colored output
        "--strict-markers",  # Strict marker checking
    ]
    
    print("=" * 60)
    print("🚀 Running Mockhaus Integration Tests")
    print("=" * 60)
    print(f"Working directory: {project_root}")
    print(f"Test directory: {project_root / 'tests' / 'integration'}")
    print(f"Command: {' '.join(cmd)}")
    print("=" * 60)
    
    try:
        # Run the tests
        result = subprocess.run(
            cmd,
            cwd=project_root,
            env=env,
            check=False  # Don't raise exception on non-zero exit
        )
        
        print("=" * 60)
        if result.returncode == 0:
            print("✅ All integration tests passed!")
        else:
            print("❌ Some integration tests failed!")
        print("=" * 60)
        
        return result.returncode
        
    except KeyboardInterrupt:
        print("\n🛑 Tests interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return 1


def run_specific_test_file(test_file: str):
    """Run a specific test file."""
    project_root = Path(__file__).parent.parent
    
    # Set environment variables for testing
    env = os.environ.copy()
    env["MOCKHAUS_ENV"] = "test"
    env["PYTHONPATH"] = str(project_root / "src")
    
    cmd = [
        sys.executable, "-m", "pytest",
        str(project_root / "tests" / "integration" / test_file),
        "-v",
        "--tb=short",
        "--color=yes",
    ]
    
    print(f"🎯 Running specific test file: {test_file}")
    
    result = subprocess.run(cmd, cwd=project_root, env=env, check=False)
    return result.returncode


def run_specific_test(test_pattern: str):
    """Run tests matching a specific pattern."""
    project_root = Path(__file__).parent.parent
    
    # Set environment variables for testing
    env = os.environ.copy()
    env["MOCKHAUS_ENV"] = "test"
    env["PYTHONPATH"] = str(project_root / "src")
    
    cmd = [
        sys.executable, "-m", "pytest",
        str(project_root / "tests" / "integration"),
        "-k", test_pattern,
        "-v",
        "--tb=short",
        "--color=yes",
    ]
    
    print(f"🔍 Running tests matching pattern: {test_pattern}")
    
    result = subprocess.run(cmd, cwd=project_root, env=env, check=False)
    return result.returncode


if __name__ == "__main__":
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg.endswith(".py"):
            # Run specific test file
            exit_code = run_specific_test_file(arg)
        else:
            # Run tests matching pattern
            exit_code = run_specific_test(arg)
    else:
        # Run all integration tests
        exit_code = run_integration_tests()
    
    sys.exit(exit_code)