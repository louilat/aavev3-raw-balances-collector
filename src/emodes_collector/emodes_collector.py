import pandas as pd
from pandas import DataFrame


class AaveV3EModesCollector:
    def __init__(self, w3, pool_abi: dict, block_number: int = "latest"):
        self.w3 = w3
        self.contract_address = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
        self.contract_abi = pool_abi
        self.pool_contract = self.w3.eth.contract(
            address=self.contract_address, abi=pool_abi
        )
        self.block_number = block_number

        self.active_users_emodes: DataFrame = DataFrame()
        self.emodes_caracteristics: DataFrame = DataFrame()

    def collect_emodes(self, users: DataFrame) -> DataFrame:
        users_ = users.copy()
        users_["snapshot_block"] = self.block_number
        users_["emode"] = 0
        for index, user in users_.iterrows():
            user_address = user["active_user_address"]
            user_emode = self.pool_contract.functions.getUserEMode(user_address).call(
                block_identifier=self.block_number
            )
            users_.loc[index, "emode"] = user_emode

        # self.active_users_emodes = users_[users_.emode != 0]
        self.active_users_emodes = users_
        return self.active_users_emodes

    def collect_emodes_configuration(self) -> DataFrame:
        emodes_ids = self.active_users_emodes.emode.unique().tolist()
        emodes_caracteristics = DataFrame()
        for emode_id in emodes_ids:
            emode_data = self.pool_contract.functions.getEModeCategoryCollateralConfig(
                emode_id
            ).call(block_identifier=self.block_number)
            label = self.pool_contract.functions.getEModeCategoryLabel(emode_id).call(
                block_identifier=self.block_number
            )
            id_data = DataFrame(
                {
                    "id": [emode_id],
                    "label": [label],
                    "loan_to_value": [emode_data[0]],
                    "liquidation_threshold": [emode_data[1]],
                    "liquidation_threshold": [emode_data[2]],
                }
            )
            emodes_caracteristics = pd.concat((emodes_caracteristics, id_data))

        self.emodes_caracteristics = emodes_caracteristics
        return self.emodes_caracteristics
