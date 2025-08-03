## Basic instructions
- Write type safe code in Python such that it passes mypy and pyright lint checks
- Run `make test` to run the tests
- Run `make lint` to run the linter
- When I make a suggestion, don't just implement it blindly. Objectively evaluate the ask. Feel free to disagree.
- When you are done implementing a feature, run `make test` and `make lint` to ensure everything is working as expected

# Code Quality Standards

## TypeScript/Python Code Requirements
  - ALWAYS use proper type annotations for all function parameters and return types
  - Use `| None` instead of `Optional[]` for nullable types
  - Mark unused parameters with `_` prefix or add `# noqa: ARG001` comment
  - Remove trailing whitespace and ensure files end with newline
  - Keep line length under 150 characters
  - Import from `collections.abc` instead of `typing` for standard collections
  - Fix all linting issues during implementation, not afterward


# Docs
- Docs must be concise and easy to read
- After you make all the code changes are done with implementing a feature, make sure to review all .md files and ensure that the docs are updated. Especially the README.md
