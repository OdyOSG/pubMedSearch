
"""
Command‑line interface powered by *click*:

$ python -m pubmed_search "Asthma"
"""
from __future__ import annotations
import click, sys, pathlib
from .client import search

@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("mesh_term", metavar="MESH_TERM")
@click.option("--year-from", type=int, default=None, help="Start publication year (inclusive).")
@click.option("--year-to", type=int, default=None, help="End publication year (inclusive).")
@click.option("--api-key", envvar="NCBI_API_KEY", help="NCBI E‑utilities API key.")
@click.option("-o", "--output", type=click.Path(dir_okay=False), default="results.xlsx",
              help="Excel file to write (default: results.xlsx).")
def cli(mesh_term: str, year_from: int | None, year_to: int | None, api_key: str | None, output: str):
    """Search PubMed for observational RWD studies about *MESH_TERM*."""
    df = search(mesh_term, year_from=year_from, year_to=year_to, api_key=api_key)
    outfile = export_dataframe_to_excel(df, output)
    click.secho(f"Wrote {len(df):,} records to {outfile}", fg="green")

if __name__ == "__main__":
    cli()
