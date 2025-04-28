
"""Tiny wrapper exposing `pubmed-search` as a `click` console script."""

from __future__ import annotations

import pathlib
import sys

import click

from .client import search
from .utils import export_dataframe_to_excel

@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("mesh_term", metavar="QUERY", nargs=-1, required=True)
@click.option("--year-from", type=int, default=None, help="Start publication year (inclusive).")
@click.option("--year-to", type=int, default=None, help="End publication year (inclusive).")
@click.option("--api-key", envvar="NCBI_API_KEY", help="NCBI E‑utilities API key.")
@click.option(
    "-o",
    "--output",
    type=click.Path(dir_okay=False, writable=True, path_type=pathlib.Path),
    default=pathlib.Path("results.xlsx"),
    help="Excel file to write (default: results.xlsx).",
)
def cli(
    query: tuple[str, ...],
    year_from: int | None,
    year_to: int | None,
    api_key: str | None,
    output: pathlib.Path,
) -> None:
    """Search PubMed for *QUERY* and export the results to an Excel workbook."""
    df = search(query, year_from=year_from, year_to=year_to, api_key=api_key)
    outfile = export_dataframe_to_excel(df, output)
    click.secho(f"Wrote {len(df):,} records to '{outfile}'.", fg="green")

if __name__ == "__main__":
    # Allows `python -m pubmed_search "asthma"`
    try:
        cli()
    except Exception as exc:
        click.secho(f"Error: {exc}", fg="red", err=True)
        sys.exit(1)
