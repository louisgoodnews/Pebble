# Pebble

A lightweight, file-based mini database/datastore toolkit for Python.  
Pebble provides **immutable records**, **tables with schema/constraints**, **filter & query strings** (with `ALL`/`ANY`/`NONE` scopes), and **builder/factory patterns** – all in plain Python, persisted as JSON.

> Core idea: You work with **dictionary-like** data structures (records, tables, “databases”) and can define them **type-safe**, **filter/query them**, **compose them**, and **commit them to file**.

---

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [Core Concepts](#core-concepts)
  - [Pebble](#pebble)
  - [PebbleDatabase](#pebbledatabase)
  - [PebbleTable](#pebbletable)
  - [PebbleField & Factory](#pebblefield--factory)
  - [PebbleRecord (immutable)](#pebblerecord-immutable)
  - [Tools & Builder](#tools--builder)
  - [Filters, Queries & Engine](#filters-queries--engine)
- [Query/Filter Syntax](#queryfilter-syntax)
- [Persistence & Files](#persistence--files)
- [Errors & Exceptions](#errors--exceptions)
- [Examples](#examples)
- [Roadmap](#roadmap)
- [License](#license)

---

## Features

- **File-based (JSON)**: Databases & tables can be read/written (`commit`).  
- **Immutable Records**: `PebbleRecord` is immutable (functional style: `update()`, `without()`). Direct mutation raises errors.  
- **Schema/Definition** for tables: fields, unique keys, references, indexes, primary keys.  
- **Filter/Query DSL**: Operators `== != < > <= >= in not in is is not`, scopes `ALL | ANY | NONE`, case flags.  
- **Filter Engine** to combine multiple filters (AND/OR) – on dicts or tables.  
- **Builder/Factory Patterns** for databases & tables.  
- **Utilities**: JSON <-> dict, traversal, immutable/mutable conversion.  

---

## Installation

Pebble is pure Python with no external dependencies (apart from standard library + `utils/*` of your project).  
Copy the module into your project or install as internal package.

```bash
git clone https://github.com/louisgoodnews/Pebble
cd Pebble
pip install -r requirements.txt
```

---

## Quickstart

```python
from pathlib import Path
from core import Pebble, PebbleTableBuilder, PebbleFilterString, PebbleQueryEngine

# 1) Create Pebble instance and table
pebble = Pebble()
users = pebble.table("users", path=Path("./users.json"))

# 2) Add entries (as dicts)
users.add({"id": 1, "name": "Alice", "age": 30})
users.add({"id": 2, "name": "Bob",   "age": 25})
users.commit()  # persist as JSON

# 3) Filtering
flt = PebbleFilterString("name == 'alice'")
engine = users.filter_engine().set_filter(flt, scope="ALL")
result = engine.filter()  # -> list of matching rows

# 4) Queries across filters/tables
qe = PebbleQueryEngine(users.to_dict())
qe.set_query("users.name == 'Alice'")
rows = qe.query()
```

---

## Core Concepts

### Pebble
Entry point & registry for **databases** and **tables**.  
- `database(name, path)`: create/get a DB.  
- `table(name, path)`: create/get a table.  
- `get_or_create_database/table(...)` loads from file if needed.  
- `empty(databases=True|False, tables=True|False)`: check emptiness of registries.  

### PebbleDatabase
Lightweight collection of tables with metadata.  
- `table(name, path)`: build + register table.  
- `tables(names=False)`: list table names or objects.  
- `commit() / delete() / from_file()` – persistence.  
- `configure('definition.*', value)`: only allows definition/constraint changes.  

### PebbleTable
A **logical table** with `definition` (schema) and `entries`.  
- `add(entry: dict, identifier: str|None) -> id`: add record.  
- `all(format='dict|list|set|tuple')`: return entries in different formats.  
- `commit() / delete() / from_file()` – persistence.  
- `check_for_size()`: protect against size limit.  
- `configure('definition.*' or 'constraints.*', value)`: update schema.  
- `to_dict()/to_json()` – serialization.  

**Definition/Schema:**  
- `fields`, `unique`, `references`, `indexes`, `primary_key`, `constraints`.  

### PebbleField & Factory
Type-safe field metadata with `choices`, `default`, `required`, `validator`.  
- Types: `BOOLEAN, DATE, DATETIME, DECIMAL, DICTIONARY, FLOAT, INTEGER, LIST, PATH, SET, STRING, TIME, TUPLE, UUID`.  
- Factory method: `PebbleFieldFactory.create(...)`.  

### PebbleRecord (immutable)
Immutable mapping-like wrapper around dicts.  
- No mutation (`__setitem__` disabled). Raises `PebbleRecordImmutabilityViolationError`.  
- Methods: `update(**kw) -> new`, `without(key) -> new`, `to_dict()/to_list()/to_tuple()` etc.  

### Tools & Builder
- **PebbleTool**: `from_json(...)`, `immutable(...)`/`mutable(...)`, `traverse(...)`.  
- **PebbleToolBuilder**: Chain operations on dicts/records:  
  - `set(path, value)`, `delete(path)`, `merge(record)`, `filter(string, scope='ALL')`, `to_immutable()`, `to_json()`.  
  - `query(...)` currently **not implemented** (raises `NotImplementedError`).  

### Filters, Queries & Engine
- **PebbleFilterString**: Single predicate (e.g., `"age >= 18"`). Evaluatable against a dict row.  
- **PebbleFilterEngine**: Combine filters (`AND`/`OR`) and scopes (`ALL|ANY|NONE`). Returns list + metadata.  
- **PebbleQueryString**: High-level query string → split into filter strings. Supports table prefixes, scopes, case flags.  
- **PebbleQueryEngine**: Holds multiple query strings and aggregates results.  

---

## Query/Filter Syntax

### FilterString
```
<field> <operator> <value>
```

- **Operators**: `==`, `!=`, `<`, `>`, `<=`, `>=`, `in`, `not in`, `is`, `is not`.  
- **Evaluation**: runs against dict row. Missing fields → no match.  

### QueryString
- Can include **multiple filters** and **table prefixes**.  
- Split into **sub-queries** and **filter strings**.  
- Supports scopes (`ALL|ANY|NONE`) and case flags (`CASE_INSENSITIVE` default).  
- Example:

```
users.name == 'Alice' AND users.age >= 18
```

---

## Persistence & Files

- **Tables & databases** can be persisted via `commit()`. Creates files if needed.  
- **Load** via `from_file(path)` (creates file if missing).  
- **Delete** via `delete()`.  

---

## Errors & Exceptions

- **Immutable violation**: Writing to `PebbleRecord` → `PebbleRecordImmutabilityViolationError`.  
- **Size limit**: `check_for_size()` raises `PebbleSizeExceededError`.  
- **Invalid filter/query format**: → `PebbleFilterStringFormatError` / `PebbleQueryStringFormatError`.  
- **Unsupported operator**: → `ValueError` in `evaluate()`.  
- **Duplicate schema entries**: Builder methods may raise `ValueError`.  

---

## Examples

### Define a table with schema

```python
from core import PebbleTableBuilder, PebbleFieldFactory

builder = PebbleTableBuilder()
builder.with_identifier().with_name("users").with_path("./users.json")

# Schema example
email_field = PebbleFieldFactory.create("email", "STRING", required=True)
age_field   = PebbleFieldFactory.create("age", "INTEGER", default=0)

tbl = builder.build()
tbl.configure("definition.fields.email", email_field.__dict__)
tbl.configure("definition.unique", {"0": "email"})

tbl.commit()
```

### Filtering with engine

```python
from core import PebbleFilterString

flt1 = PebbleFilterString("name == 'Alice'")
flt2 = PebbleFilterString("age >= 18")

eng = tbl.filter_engine().set_filters([flt1, flt2], operator="AND", scope="ALL")
res = eng.filter()
```

### Query with QueryEngine

```python
from core import PebbleQueryEngine

qe = PebbleQueryEngine(tbl.to_dict())
qe.set_query("users.name == 'Alice'")
data = qe.query()
```

### Immutable Records

```python
from core import PebbleTool, PebbleToolBuilder

rec = PebbleTool.immutable({"name": "Alice", "age": 30})
rec2 = rec.update(age=31)

b = PebbleToolBuilder({"user": {"name": "Bob"}}).set("user.age", 25).to_immutable()
final_record = b.build()
```

---

## Roadmap

- Implement `PebbleToolBuilder.query(...)` (currently `NotImplementedError`).  
- Add helper APIs for schema manipulation.  
- Apply `PebbleField.validator` automatically to entries.  

---

## License

MIT (or adapt to your project).  
Author: Louis Goodnews (2025-09-05).

---


---

## Community

We welcome contributions and feedback! 🎉

- See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to get started.  
- Please follow our [Code of Conduct](CODE_OF_CONDUCT.md) to help us keep a welcoming community.
