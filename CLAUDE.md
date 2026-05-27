# HDX Python Database

HDX Python Database Library — provides utilities for connecting to databases (including via SSH tunnel) using SQLAlchemy.

## Setup

```shell
uv sync
```

## Running Tests

```shell
uv run pytest
```

## Linting

```shell
uv run ruff check
uv run ruff format --check
```

## Building

```shell
uv build
```

## Package Management

- Runtime dependencies go in `[project.dependencies]` in `pyproject.toml`
- Dev/test dependencies go in `[dependency-groups] dev` in `pyproject.toml`
- Optional runtime extras (`sshtunnel`, `postgresql`) stay in `[project.optional-dependencies]`
- After changing dependencies, run `uv lock --upgrade` to regenerate `uv.lock`
