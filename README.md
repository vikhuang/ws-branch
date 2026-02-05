# ws-branch

A data science project.

## Requirements

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) for dependency management
- Docker (optional)

## Setup

```bash
# Install dependencies
uv sync

# Install with dev dependencies
uv sync --dev
```

## Usage

```bash
# Run the application
uv run python main.py

# Run tests
uv run pytest

# Run linting
uv run ruff check .

# Run type checking
uv run mypy src
```

## Docker

```bash
# Build and run production image
docker compose up app

# Run development environment with Jupyter
docker compose up dev
```

## Project Structure

```
ws-branch/
├── src/ws_branch/    # Source code
├── tests/            # Test files
├── notebooks/        # Jupyter notebooks
├── data/             # Data files (gitignored)
├── output/           # Output files (gitignored)
├── Dockerfile        # Docker configuration
├── docker-compose.yml
└── pyproject.toml    # Project configuration
```

## License

MIT
