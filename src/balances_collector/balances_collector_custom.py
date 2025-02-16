import pandas as pd
from pandas import DataFrame
import concurrent.futures

reserves_names_dict = {
    "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2": "Wrapped Ether",
    "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0": "Wrapped liquid staked Ether 2.0",
    "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599": "Wrapped BTC",
    "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48": "USD Coin",
    "0x6B175474E89094C44Da98b954EedeAC495271d0F": "Dai Stablecoin",
    "0x514910771AF9Ca656af840dff83E8264EcF986CA": "ChainLink Token",
    "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9": "Aave Token",
    "0xBe9895146f7AF43049ca1c1AE358B0541Ea49704": "Coinbase Wrapped Staked ETH",
    "0xdAC17F958D2ee523a2206206994597C13D831ec7": "Tether USD",
    "0xae78736Cd615f374D3085123A210448E74Fc6393": "Rocket Pool ETH",
    "0x5f98805A4E8be255a32880FDeC7F6728C6568bA0": "LUSD Stablecoin",
    "0xD533a949740bb3306d119CC777fa900bA034cd52": "Curve DAO Token",
    "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2": "Maker",
    "0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F": "Synthetix Network Token",
    "0xba100000625a3754423978a60c9317c58a424e3D": "Balancer",
    "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984": "Uniswap",
    "0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32": "Lido DAO Token",
    "0xC18360217D8F7Ab5e7c516566761Ea12Ce7F9D72": "Ethereum Name Service",
    "0x111111111117dC0aa78b770fA6A738034120C302": "1INCH Token",
    "0x853d955aCEf822Db058eb8505911ED77F175b99e": "Frax",
    "0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f": "Gho Token",
    "0xD33526068D116cE69F19A9ee46F0bd304F21A51f": "Rocket Pool Protocol",
    "0x83F20F44975D03b1b09e64809B757c47f942BEeA": "Savings Dai",
    "0xAf5191B0De278C7286d6C7CC6ab6BB8A73bA2Cd6": "StargateToken",
    "0xdeFA4e8a7bcBA345F687a2f1456F5Edd9CE97202": "Kyber Network Crystal v2",
    "0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0": "Frax Share",
    "0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E": "Curve.Fi USD Stablecoin",
    "0x6c3ea9036406852006290770BEdFcAbA0e23A0e8": "PayPal USD",
    "0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee": "Wrapped eETH",
    "0xf1C9acDc66974dFB6dEcB12aA385b9cD01190E38": "Staked ETH",
    "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3": "USDe",
    "0xA35b1B31Ce002FBF2058D22F30f95D405200A15b": "ETHx",
    "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497": "Staked USDe",
    "0x18084fbA666a33d37592fA2633fD49a74DD93a88": "tBTC v2",
    "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf": "Coinbase Wrapped BTC",
    "0xdC035D45d973E3EC169d2276DDab16f1e407384F": "USDS Stablecoin",
    "0xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7": "rsETH",
    "0x8236a87084f8B84306f72007F36F2618A5634494": "Lombard Staked Bitcoin",
}


class AaveV3RawBalancesCollectorCustom:
    def __init__(
        self,
        w3,
        pool_abi: dict,
        atoken_abi: dict,
        addresses_provider_abi: dict,
        price_oracle_abi: dict,
    ):
        # Provider
        self.w3 = w3

        # Pool contract
        self.pool_address = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
        self.pool_abi = pool_abi
        self.pool_contract = self.w3.eth.contract(
            address=self.pool_address, abi=self.pool_abi
        )

        # ABIs
        self.atoken_abi = atoken_abi
        self.addresses_provider_abi = addresses_provider_abi
        self.price_oracle_abi = price_oracle_abi

        # Computed data
        self.reserves_data: DataFrame = DataFrame()
        self.all_users_positions: DataFrame = DataFrame()
        self.processed_balances: DataFrame = DataFrame()

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

        reserves_data = pd.json_normalize(all_reserves_data)
        reserves_data["name"] = reserves_data.underlyingAsset.map(reserves_names_dict)

        # Extracting underlying token price from oracle contract
        provider_address = self.pool_contract.functions.ADDRESSES_PROVIDER().call(
            block_identifier=block_identifier
        )
        provider_contract = self.w3.eth.contract(
            address=provider_address, abi=self.addresses_provider_abi
        )
        oracle_address = provider_contract.functions.getPriceOracle().call(
            block_identifier=block_identifier
        )
        oracle_contract = self.w3.eth.contract(
            address=oracle_address, abi=self.price_oracle_abi
        )

        prices_list = oracle_contract.functions.getAssetsPrices(
            reserves_data.underlyingAsset.tolist()
        ).call(block_identifier=block_identifier)
        currency_unit = oracle_contract.functions.BASE_CURRENCY_UNIT().call(
            block_identifier=block_identifier
        )
        reserves_data["underlyingTokenPriceUSD"] = prices_list
        reserves_data.underlyingTokenPriceUSD = (
            reserves_data.underlyingTokenPriceUSD / currency_unit
        )

        self.reserves_data = reserves_data
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

        users_positions = DataFrame()
        for user, balance in all_users_positions.items():
            user_data = DataFrame(
                {
                    "user_address": user,
                    "underlyingAsset": balance["underlyingAsset"],
                    "scaledATokenBalance": balance["scaledATokenBalance"],
                    "scaledVariableDebt": balance["scaledVariableDebt"],
                }
            )
            users_positions = pd.concat((users_positions, user_data))
        users_positions["snapshot_block"] = block_identifier
        self.all_users_positions = users_positions
        return users_positions

    def process_users_balances(self):
        merge_columns = [
            "underlyingAsset",
            "name",
            "decimals",
            "baseLTVasCollateral",
            "reserveLiquidationThreshold",
            "reserveLiquidationBonus",
            "liquidityIndex",
            "variableBorrowIndex",
            "underlyingTokenPriceUSD",
        ]
        processed_balances = self.all_users_positions.merge(
            self.reserves_data[merge_columns], how="left", on="underlyingAsset"
        )

        processed_balances.liquidityIndex /= 1e27
        processed_balances.variableBorrowIndex /= 1e27

        processed_balances["currentATokenBalance"] = (
            processed_balances.scaledATokenBalance
            / 10**processed_balances.decimals
            * processed_balances.liquidityIndex
        )
        processed_balances["currentVariableDebt"] = (
            processed_balances.scaledVariableDebt
            / 10**processed_balances.decimals
            * processed_balances.variableBorrowIndex
        )

        processed_balances["currentATokenBalanceUSD"] = (
            processed_balances.currentATokenBalance
            * processed_balances.underlyingTokenPriceUSD
        )
        processed_balances["currentVariableDebtUSD"] = (
            processed_balances.currentVariableDebt
            * processed_balances.underlyingTokenPriceUSD
        )

        processed_balances = processed_balances[
            (processed_balances.currentATokenBalanceUSD > 0.05)
            | (processed_balances.currentVariableDebtUSD > 0.05)
        ]

        self.processed_balances = processed_balances
        return processed_balances

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
