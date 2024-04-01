from typing import Any

import polars as pl

from ..logger import log


def rename_df_cols(raw_df: pl.DataFrame, rename: tuple[tuple[str]]):
    log("Frame", "Renaming rows")
    return raw_df.rename({key: value for key, value in rename})


def format_day(table: pl.DataFrame) -> pl.DataFrame:
    if pl.String in table.select("day").dtypes:
        return table.with_columns(pl.col("day").str.to_datetime(time_zone="UTC"))
    return table


def merge_tables(
    left: pl.DataFrame,
    right: pl.DataFrame,
    rename: tuple[tuple[str]] = (),
    projects: tuple[str] = ("", ""),
) -> pl.DataFrame:
    log("Frame", f"Merging {projects[0]} with {projects[1]}")
    merged_df = format_day(left).join(format_day(right), on="day", how="outer")

    if len(rename):
        merged_df = rename_df_cols(merged_df, rename)
    return merged_df.drop(("day_right", "__row_index_right"))


def get_table(raw_table: list[Any]) -> pl.DataFrame:
    return pl.DataFrame(raw_table)
