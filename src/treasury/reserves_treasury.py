"""Functions for extracting reserves' treasury"""

import json
import pandas as pd
from pandas import DataFrame


with open("src/abi/atoken_abi.json") as file:
    atoken_abi = json.load(file)


def collect_reserves_treasury(
    w3, reserves_data: DataFrame, block_number: int = "latest"
) -> DataFrame:
    reserves_data["treasury_balance"] = None
    treasury_collector_address = "0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c"
    for index, row in reserves_data.iterrows():
        reserve_name = row["name"]
        if reserve_name == "Gho Token":
            atoken_address = row["underlyingAsset"]
        else:
            atoken_address = row["aTokenAddress"]
        print(f"      --> Collecting treasury balance for reserve: {reserve_name}...")
        atoken_contract = w3.eth.contract(address=atoken_address, abi=atoken_abi)
        treasury_balance = atoken_contract.functions.balanceOf(
            treasury_collector_address
        ).call(block_identifier=block_number)
        reserves_data.loc[index, "treasury_balance"] = treasury_balance
    reserves_data["treasury_balance_usd"] = (
        reserves_data.treasury_balance
        / 10**reserves_data.decimals
        * reserves_data.underlyingTokenPriceUSD
    )
    return reserves_data
