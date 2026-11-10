---
id: index
title: Contributing
sidebar_position: 1
---

# Contributing to SDP-META

We welcome contributions from the community. Whether you are fixing a bug, adding a feature, improving documentation, or writing tests, your help is appreciated.

---

## Development Setup

### 1. Fork and Clone

Fork the repository on GitHub, then clone your fork:

```bash
git clone https://github.com/<your-username>/sdp-meta.git
cd sdp-meta
```

### 2. Create a Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

:::warning
Use Python 3.10, 3.11, or 3.12. Python 3.13+ is not compatible with `pyspark==3.5.5`. See [Troubleshooting](../operations/troubleshooting#python-version-issues).
:::

### 3. Install Dependencies

```bash
# Install the project and its maintained development/test dependencies.
python -m pip install -e .
python -m pip install -r requirements-dev.txt
```

The editable install makes the package importable without setting `PYTHONPATH`.

---

## Running Unit Tests

```bash
# Run all unit tests
pytest tests/

# Run a specific test file
pytest tests/test_dataflow_pipeline.py

# Run with coverage report
coverage run -m pytest tests/
coverage report
coverage html  # generates htmlcov/index.html
```

All tests must pass before submitting a pull request.

---

## Code Style

SDP-META uses [flake8](https://flake8.pycqa.org/) for linting:

```bash
flake8 src/ tests/
```

Key style guidelines:

- Maximum line length: 120 characters
- Follow PEP 8 naming conventions
- Add docstrings to public classes and methods
- Keep functions focused and testable

---

## Submitting a Pull Request

1. **Create a branch** from `main`:

   ```bash
   git checkout -b feature/my-feature-name
   ```

2. **Make your changes** and write tests covering the new behavior.

3. **Run the test suite and linter** to confirm everything passes:

   ```bash
   flake8 src/ tests/
   pytest tests/
   ```

4. **Commit your changes** with a descriptive commit message.

5. **Push your branch** and open a pull request against the `main` branch on the upstream repository.

6. **Describe your change** in the PR description — what problem it solves and how it was tested.

A maintainer will review your PR and may request changes before merging.

---

## Reporting Issues

Use [GitHub Issues](https://github.com/databrickslabs/sdp-meta/issues) to report bugs or request features.

When reporting a bug, please include:

- SDP-META version (`pip show databricks-labs-sdp-meta`)
- Python version (`python --version`)
- Databricks Runtime version
- A minimal reproducible example or the full error traceback
- The relevant section of your onboarding file (with sensitive values redacted)
