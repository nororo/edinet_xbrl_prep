"""Saving tables and writing data lineage JSON (``{output file name}_lin.json``).

Lineage items:

- file_name: file name (full path)
- purpose: purpose
- source_code: code that created the data (``package/module.py::function``)
- date_of_source_code: last modified date of that code (the last commit date if tracked by git and unchanged,
  otherwise the file modification date)
- source_data_list: source data (a large set such as many documents is described rather than listed)
- record_num: number of rows
- schema: column names
- record_example: first row (column names and values)
- data_structure: description of the data structure
- importance_columns: important columns and their meaning
- additional: sha256 (of the output file), created_at, package_version, config (run settings)
"""

from __future__ import annotations

import datetime as dt
import hashlib
import inspect
import json
import math
import subprocess
from collections.abc import Callable, Mapping, Sequence
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pandera.pandas as pa

PACKAGE_DIR = Path(__file__).resolve().parent
LINEAGE_SUFFIX = "_lin.json"
REQUIRED_KEYS = (
    "file_name",
    "purpose",
    "source_code",
    "date_of_source_code",
    "source_data_list",
    "record_num",
    "schema",
    "record_example",
    "data_structure",
    "importance_columns",
)
EXAMPLE_MAX_CHARS = 300  # long strings in record_example (texts, etc.) are cut at this length


class LineageError(ValueError):
    """A required lineage item is missing or invalid."""


def to_json_value(value: Any) -> Any:
    """Convert numpy/pandas values to JSON-serializable values; missing values become None."""
    if isinstance(value, Mapping):
        return {str(k): to_json_value(v) for k, v in value.items()}
    if isinstance(value, list | tuple | np.ndarray):
        return [to_json_value(v) for v in value]
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, pd.Timestamp | dt.datetime | dt.date):
        return value.isoformat()
    if isinstance(value, str) and len(value) > EXAMPLE_MAX_CHARS:
        return value[:EXAMPLE_MAX_CHARS] + "…"
    if isinstance(value, str | int | float | bool):
        return value
    return str(value)


def lineage_path(output_path: str | Path) -> Path:
    output_path = Path(output_path)
    return output_path.with_name(output_path.stem + LINEAGE_SUFFIX)


def source_code_date(file: Path) -> str:
    """Last modified date of the code (YYYY-MM-DD)."""
    def git(*args: str) -> str | None:
        try:
            out = subprocess.run(
                ["git", "-C", str(file.parent), *args], capture_output=True, text=True, check=True
            )
        except (OSError, subprocess.CalledProcessError):
            return None
        return out.stdout.strip()

    if git("status", "--porcelain", "--", file.name) == "":
        committed = git("log", "-1", "--format=%cs", "--", file.name)
        if committed:
            return committed
    return dt.date.fromtimestamp(file.stat().st_mtime).isoformat()


def describe_source_code(source: Callable | str) -> tuple[str, str | None]:
    """Function -> (``edinet_xbrl_prep/fs_tbl.py::build_fs_table``, last modified date). A string is returned as is."""
    if isinstance(source, str):
        return source, None
    file = Path(inspect.getfile(source)).resolve()
    label = f"{PACKAGE_DIR.name}/{file.relative_to(PACKAGE_DIR)}" if file.is_relative_to(PACKAGE_DIR) else str(file)
    return f"{label}::{source.__qualname__}", source_code_date(file)


def package_version() -> str | None:
    try:
        return version("edinet-xbrl-prep")
    except PackageNotFoundError:
        return None


def build_lineage(
    output_path: str | Path,
    data: pd.DataFrame,
    *,
    purpose: str,
    source_code: Callable | str,
    source_data_list: Sequence[str],
    data_structure: str,
    importance_columns: Mapping[str, str],
    run_config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    output_path = Path(output_path).resolve()
    source_label, source_date = describe_source_code(source_code)
    lineage = {
        "file_name": str(output_path),
        "purpose": purpose,
        "source_code": source_label,
        "date_of_source_code": source_date or dt.date.today().isoformat(),
        "source_data_list": [str(s) for s in source_data_list],
        "record_num": int(len(data)),
        "schema": [str(c) for c in data.columns],
        "record_example": to_json_value(data.iloc[0].to_dict()) if len(data) else {},
        "data_structure": data_structure,
        "importance_columns": dict(importance_columns),
        "sha256": hashlib.sha256(output_path.read_bytes()).hexdigest() if output_path.exists() else None,
        "created_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        "package_version": package_version(),
        "config": to_json_value(dict(run_config or {})),
    }
    validate_lineage(lineage)
    return lineage


def validate_lineage(lineage: Mapping[str, Any]) -> None:
    """Check that the 10 required items are present and not empty."""
    missing = [k for k in REQUIRED_KEYS if k not in lineage]
    if missing:
        raise LineageError(f"必須項目がない: {missing}")
    empty = [k for k in REQUIRED_KEYS if k not in ("record_num", "record_example") and not lineage[k]]
    if empty:
        raise LineageError(f"必須項目が空: {empty}")
    if not isinstance(lineage["record_num"], int) or lineage["record_num"] < 0:
        raise LineageError("record_num は0以上の整数")


def read_table(path: str | Path) -> pd.DataFrame:
    """Read a table saved by ``save_table`` (.parquet, .csv, .jsonl)."""
    path = Path(path)
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    if path.suffix == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
    if path.suffix == ".jsonl":
        return pd.read_json(path, lines=True, dtype=False)
    raise ValueError(f"対応していない形式: {path.suffix}")


def save_table(
    data: pd.DataFrame,
    path: str | Path,
    *,
    schema: type[pa.DataFrameModel] | None,
    purpose: str,
    source_code: Callable | str,
    source_data_list: Sequence[str],
    data_structure: str,
    importance_columns: Mapping[str, str],
    run_config: Mapping[str, Any] | None = None,
) -> Path:
    """Validate and save a table, write its lineage (``{name}_lin.json``) and return the lineage path.

    The format is chosen by the extension (.parquet, .csv, .jsonl).
    """
    path = Path(path)
    if schema is not None:
        data = schema.validate(data)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".parquet":
        data.to_parquet(path, index=False)
    elif path.suffix == ".csv":
        data.to_csv(path, index=False)
    elif path.suffix == ".jsonl":
        data.to_json(path, orient="records", lines=True, force_ascii=False)
    else:
        raise ValueError(f"対応していない形式: {path.suffix}")
    lineage = build_lineage(
        path,
        data,
        purpose=purpose,
        source_code=source_code,
        source_data_list=source_data_list,
        data_structure=data_structure,
        importance_columns=importance_columns,
        run_config=run_config,
    )
    target = lineage_path(path)
    target.write_text(json.dumps(lineage, ensure_ascii=False, indent=2), encoding="utf-8")
    return target
