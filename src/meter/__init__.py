from pathlib import Path
from os import environ

from flipside import Flipside
from dotenv import load_dotenv

from .config import sol_rename_mappings
from .frame import get_table, merge_tables
from .logger import log
from .query import get_raw_table

load_dotenv()


def main():
    # 1) set up environment
    flipside = Flipside(environ.get("FLIPSIDE"), "https://api-v2.flipsidecrypto.xyz")

    eth_defi = get_table(get_raw_table(flipside, "eth"))
    sol_ray = get_table(get_raw_table(flipside, "solRay"))
    sol_slnd = get_table(get_raw_table(flipside, "solSlnd"))

    # 5) process
    sol_defi = merge_tables(
        sol_ray, sol_slnd, sol_rename_mappings, ("Raydium", "Solend")
    )
    eth_v_sol = merge_tables(
        sol_defi, eth_defi, projects=("Solana DeFi", "Ethereum DeFi")
    )
    print(eth_v_sol.describe())

    # 6) export to parquet
    log("Export", "Writing to Parquet")
    eth_v_sol.write_parquet(
        Path(__file__).parent.resolve().joinpath("data", "eth_v_sol.parquet")
    )
