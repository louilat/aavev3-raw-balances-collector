import pandas as pd
from pandas import DataFrame
import concurrent.futures


class AaveV3RawBalancesCollectorCustom:
    def __init__(self, w3, pool_abi: dict, atoken_abi: dict):
        # Provider
        self.w3 = w3

        # Pool contract
        self.pool_address = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
        self.pool_abi = pool_abi
        self.pool_contract = self.w3.eth.contract(
            address=self.pool_address, abi=self.pool_abi
        )

        # AToken abi
        self.atoken_abi = atoken_abi

        # Computed data
        self.reserves_data: DataFrame = DataFrame()

    def get_reserves_data(self, block_identifier: int) -> DataFrame:
        all_reserves_data = list()
        reserves_list = self.pool_contract.functions.getReservesList().call(
            block_identifier=block_identifier
        )
        for underlying_asset_address in reserves_list:
            reserve_data = self.pool_contract.functions.getReserveData(
                underlying_asset_address
            ).call(block_identifier=block_identifier)
            configuration = self._read_reserve_configuration(reserve_data[0][0])
            data = self._read_reserve_data(
                block_identifier, underlying_asset_address, reserve_data
            )
            configuration.update(data)
            all_reserves_data.append(configuration)

        self.reserves_data = pd.json_normalize(all_reserves_data)
        return self.reserves_data

    def get_all_users_position(self, users: list, block_identifier: int):
        all_users_positions = dict()
        users_with_error = list()

        # Creating atoken and vtoken contracts for each reserve
        reserves_contracts = dict()
        for _, row in self.reserves_data.iterrows():
            atoken_contract = self.w3.eth.contract(
                address=row["aTokenAddress"], abi=self.atoken_abi
            )
            vtoken_contract = self.w3.eth.contract(
                address=row["variableDebtTokenAddress"], abi=self.atoken_abi
            )
            reserves_contracts.update(
                {row["underlyingAsset"]: [atoken_contract, vtoken_contract]}
            )

        # Extracting position for each user
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(
                    self._get_user_position, reserves_contracts, user, block_identifier
                ): user
                for user in users
            }
            for future in concurrent.futures.as_completed(futures):
                user = futures[future]
                try:
                    result = future.result()
                    print(f"Result: {result}")
                    all_users_positions.update({user: result})
                except:
                    print(f"Found an error for user: {user}")
                    users_with_error.append(user)

        self.all_users_positions_dict = all_users_positions
        return all_users_positions

    def _read_reserve_configuration(self, configuration: int) -> dict:
        binary = bin(configuration)
        configuration_dict = {
            "baseLTVasCollateral": int(binary[-15:], 2),
            "reserveLiquidationThreshold": int(binary[-31:-16], 2),
            "reserveLiquidationBonus": int(binary[-47:-32], 2),
            "decimals": int(binary[-55:-48], 2),
            "isActive": bool(int(binary[-56], 2)),
            "isFrozen": bool(int(binary[-57], 2)),
            "borrowingEnabled": bool(int(binary[-58], 2)),
            "reserveFactor": int(binary[-79:-64], 2),
        }
        return configuration_dict

    def _read_reserve_data(
        self, block_identifier: int, underlying_asset_address: str, reserve_data: tuple
    ) -> dict:
        reserve_data_dict = {
            "underlyingAsset": underlying_asset_address,
            "liquidityIndex": reserve_data[1],
            "variableBorrowIndex": reserve_data[3],
            "liquidityRate": reserve_data[2],
            "variableBorrowRate": reserve_data[4],
            "lastUpdateTimestamp": reserve_data[6],
            "aTokenAddress": reserve_data[8],
            "variableDebtTokenAddress": reserve_data[10],
            "interestRateStrategyAddress": reserve_data[11],
            "accruedToTreasury": reserve_data[12],
        }
        underlying_asset_contract = self.w3.eth.contract(
            address=underlying_asset_address, abi=self.atoken_abi
        )
        vtoken_contract = self.w3.eth.contract(
            address=reserve_data_dict["variableDebtTokenAddress"], abi=self.atoken_abi
        )
        availableLiquidity = underlying_asset_contract.functions.balanceOf(
            reserve_data_dict["aTokenAddress"]
        ).call(block_identifier=block_identifier)
        totalScaledVariableDebt = vtoken_contract.functions.scaledTotalSupply().call(
            block_identifier=block_identifier
        )
        reserve_data_dict.update(
            {
                "availableLiquidity": availableLiquidity,
                "totalScaledVariableDebt": totalScaledVariableDebt,
            }
        )
        return reserve_data_dict

    def _get_user_position(
        self, reserves_contracts: dict, user_address: str, block_identifier: int
    ):
        print(f"   --> Extracting position for user: {user_address}")
        assets = list()
        atoken_balances = list()
        vtoken_balances = list()
        for underlying_asset, contracts in reserves_contracts.items():
            scaledATokenBalance = (
                contracts[0]
                .functions.scaledBalanceOf(user_address)
                .call(block_identifier=block_identifier)
            )
            scaledVariableDebt = (
                contracts[1]
                .functions.scaledBalanceOf(user_address)
                .call(block_identifier=block_identifier)
            )
            if (scaledATokenBalance > 0) or (scaledVariableDebt > 0):
                assets.append(underlying_asset)
                atoken_balances.append(scaledATokenBalance)
                vtoken_balances.append(scaledVariableDebt)
        user_position = {
            "underlyingAsset": assets,
            "scaledATokenBalance": atoken_balances,
            "scaledVariableDebt": vtoken_balances,
        }

        return user_position
