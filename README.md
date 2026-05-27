[![Build Status](https://github.com/OCHA-DAP/hdx-python-database/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-python-database/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-python-database/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-python-database?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Downloads](https://img.shields.io/pypi/dm/hdx-python-database.svg)](https://pypistats.org/packages/hdx-python-database)

The HDX Python Database Library provides utilities for connecting to databases in a standard way including
through an ssh tunnel if needed. It is built on top of SQLAlchemy and simplifies its setup.

For more information, please read the [documentation](https://hdx-python-database.readthedocs.io/en/latest/).

This library is part of the [Humanitarian Data Exchange](https://data.humdata.org/) (HDX) project. If you have
humanitarian related data, please upload your datasets to HDX.

# Development

## Environment

Development is currently done using Python 3.13. The environment can be created with:

```shell
    uv sync
```

This creates a .venv folder with the versions specified in the project's uv.lock file.

### Pre-commit

pre-commit will be installed when syncing uv. It is run every time you make a git
commit if you call it like this:

```shell
    pre-commit install
```

With pre-commit, all code is formatted according to
[ruff](https://docs.astral.sh/ruff/) guidelines.

To check if your changes pass pre-commit without committing, run:

```shell
    pre-commit run --all-files
```

## Packages

[uv](https://github.com/astral-sh/uv) is used for package management. If
you've introduced a new package to the source code (i.e. anywhere in `src/`),
please add it to the `project.dependencies` section of `pyproject.toml` with
any known version constraints.

To add packages required only for testing, add them to the
`[dependency-groups]`.

Any changes to the dependencies will be automatically reflected in
`uv.lock` with `pre-commit`, but you can re-generate the files without committing by
executing:

```shell
    uv lock --upgrade
```

## Project

[uv](https://github.com/astral-sh/uv) is used for project management. The project can be
built using:

```shell
    uv build
```

Linting and syntax checking can be run with:

```shell
    uv run ruff check
```

To run the tests and view coverage, execute:

```shell
    uv run pytest
```

## Documentation

The documentation, including API documentation, is generated using ReadtheDocs and
MkDocs with Material. As you change the source code, remember to update the
documentation at `documentation/index.md`.
