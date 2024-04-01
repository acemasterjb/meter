from pathlib import Path
from typing import Any

from flipside import Flipside, QueryResultSet

from ..logger import log


def print_stats(result: QueryResultSet):
    raw_stats = result.run_stats
    stats = {
        key: value
        for key, value in (
            ("exec_time", raw_stats.query_exec_seconds),
            ("rows", raw_stats.record_count),
            ("query_size", raw_stats.bytes),
        )
    }

    log("Query, Flipside", "".join("Stats: ", str(stats)))


def get_raw_table(
    session: Flipside, path: str, _print_stats: bool = False
) -> list[Any]:
    # 2) parse sql file(s)
    log("Query, Flipside", f"Getting table for `{path}.sql` query")
    with open(
        Path(__file__).parent.resolve().joinpath(path + "_query.sql"), "r"
    ) as raw_query:
        # 3) make Flipside call
        result = session.query(raw_query.read())

        if _print_stats:
            print_stats()

        # 4) parse into polars
        return result.records
