# Copilot Cloud Agent Instructions

## Repository Overview

**cloudwatch-logstream-cleaner** is a small Python CLI utility that deletes AWS CloudWatch log streams older than a specified number of days. It supports dry-run mode, configurable batch processing to avoid API throttling, and flexible age calculation (by creation time or last event time).

The entire tool is a **single Python file** (`main.py`). Tests live in `test_main.py`. There is no package hierarchy.

---

## Repository Structure

```
main.py           # Single-file CLI entry point — all logic lives here
test_main.py      # Unit tests (unittest-style, executed via pytest)
pyproject.toml    # Project metadata, dependencies, and ruff config
uv.lock           # Locked dependency graph (committed; do not edit manually)
.python-version   # Pins Python to 3.14
README.md         # Usage documentation
```

---

## Environment Setup

The project uses **[uv](https://github.com/astral-sh/uv)** as its Python package and environment manager.

If `uv` is not available in the shell, install it first:
```bash
pip install uv
```

Install all dependencies (including dev):
```bash
uv sync --all-groups
```

> **Note:** `uv` will automatically download Python 3.14 (pinned in `.python-version`) if it is not already installed.

---

## Running the Tool

```bash
uv run python main.py -l <log-group-name> -r <retention-days> [options]
```

Example:
```bash
uv run python main.py -l /aws/lambda/my-function -r 30 --dry-run
```

Key flags:
- `--dry-run` — simulate deletions without making changes
- `--yes` — skip the interactive confirmation prompt
- `--use-last-event` — base age on `lastEventTimestamp` instead of `creationTime`
- `--region` — override AWS region (also reads `AWS_REGION` env var)
- `--batch-size` / `--batch-pause` — control pacing to avoid throttling

---

## Testing

Tests use Python's built-in `unittest` framework but are discovered and run via `pytest`.

```bash
uv run python -m pytest test_main.py -v
```

All 7 tests should pass. Tests mock the boto3 client entirely — no real AWS credentials are needed to run them.

---

## Linting

The project uses **[ruff](https://docs.astral.sh/ruff/)** with `line-length = 120`.

```bash
uv run ruff check main.py test_main.py
```

To auto-fix fixable issues:
```bash
uv run ruff check --fix main.py test_main.py
```

### Known Pre-existing Lint Warnings

Two pre-existing `F541` (f-string without placeholders) warnings exist in `main.py` (lines 108 and 225). These are in the original code and can be fixed with `--fix`, but they are not introduced by new changes. Do not count them as regressions.

---

## Dependencies

Managed via `uv` with the lockfile `uv.lock`. Update dependencies with:
```bash
uv add <package>           # runtime dependency
uv add --dev <package>     # dev-only dependency
```

Do **not** edit `uv.lock` manually.

Runtime dependencies:
- `boto3>=1.42.78` — AWS SDK
- `boto3-stubs~=1.42.78` — Type stubs for boto3

Dev dependencies:
- `ruff>=0.15.8` — Linter
- `pytest>=9.1.1` — Test runner

---

## AWS Credentials

The tool relies on boto3's standard credential resolution chain (no credentials are committed or hardcoded):
1. `--region` CLI argument
2. `AWS_REGION` environment variable
3. `~/.aws/credentials` / `~/.aws/config`
4. EC2/ECS instance role

---

## Code Conventions

- All code is in `main.py`; keep the single-file structure
- Functions are small and focused; add new behavior as new functions
- Logging uses Python's `logging` module (not `print`); boto3/botocore/urllib3 loggers are suppressed to `WARNING` to reduce noise
- Timestamps from CloudWatch are in **milliseconds** — divide by 1000 before converting to `datetime`
- Error handling catches `ClientError` and `BotoCoreError` from `botocore.exceptions`; `ThrottlingException` triggers a 2-second sleep and one retry
- Line length limit: **120 characters**

---

## No CI Configured

There are no GitHub Actions workflows in this repository. Validation must be run locally using the commands above.

---

## Known Errors and Workarounds

| Error | Cause | Workaround |
|---|---|---|
| `uv: command not found` | `uv` is not installed in the agent environment | Run `pip install uv` before any `uv` commands |
| `No module named pytest` | `pytest` was not included in `pyproject.toml` originally | It has since been added to the `dev` dependency group; run `uv sync --all-groups` |
