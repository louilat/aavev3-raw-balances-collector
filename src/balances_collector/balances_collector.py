"""Class for extracting and processing users reserves data"""

import pandas as pd
from pandas import DataFrame
from web3 import Web3


class AaveV3RawBalancesCollector:
    def __init__(self, provider_url: str, contract_abi: dict):
        self.provider_url = provider_url
        self.w3 = Web3(Web3.HTTPProvider(provider_url))
        if self.w3.is_connected():
            print("Successfully connected to provider")
        else:
            raise Exception("Could not connect to provider")

        self.contract_address = "0x3F78BBD206e4D3c504Eb854232EdA7e47E9Fd8FC"
        self.contract_abi = contract_abi
        self.data_provider_contract = self.w3.eth.contract(
            address=self.contract_address, abi=contract_abi
        )

        self.all_users_balances: DataFrame = DataFrame()
        self.reserves_data: DataFrame = DataFrame()
        self.processed_balances: DataFrame = DataFrame()

    def collect_raw_balances(self, users: DataFrame):
        user_reserve_columns = [
            "underlyingAsset",
            "scaledATokenBalance",
            "usageAsCollateralEnabledOnUser",
            "scaledVariableDebt",
        ]
        all_users_balances = DataFrame()
        for _, user in users.iterrows():
            user_address = user["active_user_address"]
            response = self.data_provider_contract.functions.getUserReservesData(
                "0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e", user_address
            ).call()[0]
            user_data_table = DataFrame(response, columns=user_reserve_columns)
            all_users_balances = pd.concat((all_users_balances, user_data_table))

        self.all_users_balances = all_users_balances
        return all_users_balances

    def collect_reserves_data(self) -> dict:
        reserve_data_columns = [
            "underlyingAsset",
            "name",
            "symbol",
            "decimals",
            "baseLTVasCollateral",
            "reserveLiquidationThreshold",
            "reserveLiquidationBonus",
            "reserveFactor",
            "usageAsCollateralEnabled",
            "borrowingEnabled",
            "isActive",
            "isFrozen",
            "liquidityIndex",
            "variableBorrowIndex",
            "liquidityRate",
            "variableBorrowRate",
            "lastUpdateTimestamp",
        ]
        response, _ = self.data_provider_contract.functions.getReservesData(
            "0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e"
        ).call()
        response = [reserve_data[0:17] for reserve_data in response]
        reserves_data = DataFrame(response, columns=reserve_data_columns)

        self.reserves_data = reserves_data
        return reserves_data

    def process_raw_balances(self):
        merge_columns = [
            "underlyingAsset",
            "name",
            "symbol",
            "decimals",
            "baseLTVasCollateral",
            "reserveLiquidationThreshold",
            "reserveLiquidationBonus",
            "usageAsCollateralEnabled",
            "liquidityIndex",
            "variableBorrowIndex",
        ]
        processed_balances = self.all_users_balances.merge(
            self.reserves_data[merge_columns], how="left", on="underlyingAsset"
        )

        processed_balances.liquidityIndex /= 1e27
        processed_balances.variableBorrowIndex /= 1e27

        processed_balances["currentATokenBalance"] = (
            processed_balances.scaledATokenBalance
            / processed_balances.decimals
            * processed_balances.liquidityIndex
        )
        processed_balances["scaledVariableDebt"] = (
            processed_balances.scaledVariableDebt
            / processed_balances.decimals
            * processed_balances.variableBorrowIndex
        )

        self.processed_balances = processed_balances
        return processed_balances
