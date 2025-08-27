## INITIAL CONFIGURATION TO CREATE DOCUMENTATION

# Project setup

## 1. Install Python
- Use version 3.11
- Download: https://www.python.org/downloads/release/python-3119/
- During installation, check "Add Python to PATH"

Check installation:
python --version

## 2. Install Poetry
PowerShell:
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -

Check:
poetry --version

## 3. Create virtual environment
Inside project folder:
poetry env use python
poetry install

Activate environment:
poetry shell
