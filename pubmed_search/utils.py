
"""Utility helpers (Excel export, dataframe prettifying, logging)."""

from __future__ import annotations

import pathlib
from typing import Union

import pandas as pd

_XL_ENGINE = None
try:
    import xlsxwriter  # noqa: F401

    _XL_ENGINE = "xlsxwriter"
except ModuleNotFoundError:
    try:
        import openpyxl  # noqa: F401

        _XL_ENGINE = "openpyxl"
    except ModuleNotFoundError as exc:
        raise ImportError(
            "Install either 'xlsxwriter' or 'openpyxl' to enable Excel export."
        ) from exc


def _autofit_columns(worksheet, dataframe: pd.DataFrame) -> None:
    """Best‑guess column widths based on max string length per column."""
    for idx, col in enumerate(dataframe.columns):
        series = dataframe[col].astype(str)
        width = max(series.map(len).max(), len(col)) + 2
        worksheet.set_column(idx, idx, width)


def export_dataframe_to_excel(
    df: pd.DataFrame,
    path: Union[str, pathlib.Path] = "results.xlsx",
    *,
    sheet_name: str = "Results",
) -> pathlib.Path:
    """Write *df* to an XLSX file with frozen header and autofilters.

    Returns the final **Path** of the written file so callers can `print`
    or log it with ease.
    """

    path = pathlib.Path(path).expanduser().with_suffix(".xlsx")
    with pd.ExcelWriter(path, engine=_XL_ENGINE) as writer:  # type: ignore[arg-type]
        df.to_excel(writer, index=False, sheet_name=sheet_name)

        # Light‑touch formatting if using xlsxwriter
        if _XL_ENGINE == "xlsxwriter":
            workbook = writer.book
            worksheet = writer.sheets[sheet_name]  # type: ignore[index]

            # Freeze header row + enable autofilter
            worksheet.freeze_panes(1, 0)
            worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)

            # Autosize columns
            _autofit_columns(worksheet, df)

    return path
