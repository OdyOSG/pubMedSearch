
"""
Post‑processing helpers (completely optional).

These are **deliberately minimal** and kept free of heavyweight Spark /
Databricks deps.  Advanced ETL belongs in user land.
"""
from __future__ import annotations
import tempfile, os
import pandas as pd

def clean_dataframe(df: pd.DataFrame, *, drop_columns: list[str] | None = None) -> pd.DataFrame:
    """Replace NaNs / 'N/A' with empty string and drop unwanted cols."""
    clean = df.copy()
    clean.replace({"N/A": ""}, inplace=True)
    if drop_columns:
        clean.drop(columns=[c for c in drop_columns if c in clean.columns], inplace=True, errors="ignore")
    return clean

def export_dataframe_to_excel(
    df: pd.DataFrame,
    outfile: str | os.PathLike,
    *,
    sheet_name: str = "Sheet1",
) -> str:
    """
    Save *df* as a nicely formatted Excel workbook (requires **xlsxwriter**).

    Returns the absolute path for convenience.
    """
    import xlsxwriter  # runtime import so package stays optional

    df = clean_dataframe(df)
    outfile = os.fspath(outfile)
    with pd.ExcelWriter(outfile, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1, header=False)

        wb  = writer.book
        ws  = writer.sheets[sheet_name]
        (max_row, max_col) = df.shape

        # Add a table so filters are available by default
        table_cols = [{"header": col} for col in df.columns]
        ws.add_table(0, 0, max_row, max_col - 1, {"columns": table_cols, "style": "TableStyleMedium2"})

        # Basic formatting
        for i, column in enumerate(df.columns):
            col_width = max(15, int(df[column].astype(str).str.len().quantile(0.9)))
            ws.set_column(i, i, min(col_width, 60))

    return os.path.abspath(outfile)
