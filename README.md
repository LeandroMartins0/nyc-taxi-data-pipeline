
````markdown
# Project setup

## 1. Install Python
- Use version **3.11**
- Download: https://www.python.org/downloads/release/python-3119/
- During installation, check **"Add Python to PATH"**

Check installation:
```powershell
python --version
````

---

## 2. Install Poetry

Run in PowerShell:

```powershell
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -
```

Check installation:

```powershell
poetry --version
```

---

## 3. Install Java 17 (required for PySpark)

* Download JDK 17 (Temurin):
  [https://adoptium.net/temurin/releases/?version=17](https://adoptium.net/temurin/releases/?version=17)

* During installation, check the option **"Add to PATH"**

Verify installation:

```powershell
java -version
```

Expected output should show something like:

```
openjdk version "17.0.x"
```

---

## 4. Create virtual environment

Inside the project folder:

```powershell
poetry env use python
poetry install
```

Activate environment:

```powershell
poetry shell
```

---

## 5. Configure PySpark to use Poetry's Python

Find the path to Poetry’s Python executable:

```powershell
poetry run python -c "import sys; print(sys.executable)"
```

Set it as environment variable (example for PowerShell):

```powershell
$env:PYSPARK_PYTHON="C:\Users\<user>\AppData\Local\pypoetry\Cache\virtualenvs\<project-name>\Scripts\python.exe"
```

```
