.PHONY: help install dev test lint format clean

help:
	@echo "Available commands:"
	@echo "  make install    Install project dependencies"
	@echo "  make dev        Install development dependencies"
	@echo "  make test       Run tests"
	@echo "  make lint       Run linting checks"
	@echo "  make format     Format code"
	@echo "  make clean      Clean up generated files"

install:
	uv pip install -e .

dev:
	uv pip install -e ".[dev]"

test:
	pytest tests/ -v

lint:
	ruff check src/ tests/
	mypy src/

format:
	ruff format src/ tests/

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +