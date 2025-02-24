"""ETL for extracting users balances"""

import boto3
import os
import io
import pandas as pd
from datetime import datetime, timedelta, timezone
import json
from web3 import Web3
from src.balances_collector.balances_collector import AaveV3RawBalancesCollector
from src.emodes_collector.emodes_collector import AaveV3EModesCollector
from src.treasury.reserves_treasury import collect_reserves_treasury
from src.utils.block_finder_functions import find_closest_block

# Run parameters
output_path = None
pool_users_list_input_path = None
block_number = None

print("Starting ETL...")

AWS_ACCESS_KEY = os.environ["AWS_ACCESS_KEY"]
AWS_SECRET_KEY = os.environ["AWS_SECRET_KEY"]
PROVIDER_URL = os.environ["PROVIDER_URL"]
AWS_API_ENDPOINT = "https://minio-simple.lab.groupe-genes.fr"
BUCKET = "projet-datalab-group-jprat"
VERIFY = False

client_s3 = boto3.client(
    "s3",
    endpoint_url=AWS_API_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    verify=VERIFY,
)

w3 = Web3(Web3.HTTPProvider(PROVIDER_URL))
if w3.is_connected():
    print("Successfully connected to provider")
else:
    raise Exception("Could not connect to provider")


if pool_users_list_input_path is None:
    current_block = w3.eth.get_block_number()
    snapshot_day = datetime.today() - timedelta(days=14)
    snapshot_day = datetime(
        snapshot_day.year, snapshot_day.month, snapshot_day.day, tzinfo=timezone.utc
    )
    snapshot_date = snapshot_day.strftime("%Y-%m-%d")

    block_number = find_closest_block(
        w3=w3,
        target_timestamp=(snapshot_day + timedelta(days=1)).timestamp(),
        initial_block=current_block,
        step=5_000,
        verbose=True,
    )
    pool_users_list_input_path = f"aave-raw-datasource/daily-decoded-events/decoded_events_snapshot_date={snapshot_date}/all_active_users.csv"
    atoken_users_list_input_path = f"aave-raw-datasource/daily-decoded-events/decoded_events_snapshot_date={snapshot_date}/all_atoken_transfer_users.csv"
    output_path = f"aave-raw-datasource/daily-users-balances/users_balances_snapshot_date={snapshot_date}/"

print(f"Date = {snapshot_date}, Snapshot block = {block_number}")

print("STEP 0: Extracting and collecting data...")

print("   --> Extracting UIPoolDataProvider abi...")
with open("./src/abi/ui_pool_data_provider.json") as file:
    data_provider_abi = json.load(file)

print("   --> Extracting Pool abi...")
with open("./src/abi/pool_abi.json") as file:
    pool_abi = json.load(file)

print("   --> Extracting pool users list...")
object = client_s3.get_object(Bucket=BUCKET, Key=pool_users_list_input_path)
pool_users_data = pd.read_csv(object["Body"])

print("   --> Extracting atoken transfers users list...")
object = client_s3.get_object(Bucket=BUCKET, Key=atoken_users_list_input_path)
atoken_users_data = pd.read_csv(object["Body"])

all_users = pd.concat((pool_users_data, atoken_users_data)).reset_index(drop=True)

print("STEP 1: Collecting pool users balances...")

print("   --> Pool users...")

collector = AaveV3RawBalancesCollector(
    w3=w3,
    contract_abi=data_provider_abi,
    block_number=block_number,
)

collector.collect_raw_balances(pool_users_data)

print("   --> AToken Transfer users...")

atoken_collector = AaveV3RawBalancesCollector(
    w3=w3,
    contract_abi=data_provider_abi,
    block_number=block_number,
)

atoken_collector.collect_raw_balances(atoken_users_data)

print("STEP 2: Collecting reserves data...")

collector.collect_reserves_data()

atoken_collector.collect_reserves_data()

print("STEP 3: Processing users balances...")

collector.process_raw_balances()

atoken_collector.process_raw_balances()

print("STEP 4: Collecting and matching reserves treasury with reserves data...")

reserves_data = collect_reserves_treasury(
    w3=w3, reserves_data=collector.reserves_data, block_number=block_number
)

print("STEP 5: Collecting users emodes...")

emodes_collector = AaveV3EModesCollector(
    w3=w3, pool_abi=pool_abi, block_number=block_number
)

print("   --> Collecting users emodes ids")

emodes_collector.collect_emodes(all_users)

print("   --> Collecting emodes configuration")

emodes_collector.collect_emodes_configuration()

print("STEP 6: Uploading outputs to s3...")

print("   --> Pool users balances")

buffer = io.StringIO()
collector.processed_balances.to_csv(buffer, index=False)
client_s3.put_object(
    Body=buffer.getvalue(),
    Bucket=BUCKET,
    Key=output_path + "active_users_balances.csv",
)

print("   --> AToken transfers users balances")

buffer = io.StringIO()
atoken_collector.processed_balances.to_csv(buffer, index=False)
client_s3.put_object(
    Body=buffer.getvalue(),
    Bucket=BUCKET,
    Key=output_path + "atoken_transfer_users_balances.csv",
)

print("   --> Reserves data")

buffer = io.StringIO()
reserves_data.to_csv(buffer, index=False)
client_s3.put_object(
    Body=buffer.getvalue(), Bucket=BUCKET, Key=output_path + "reserves_data.csv"
)

print("   --> Active users emodes")

buffer = io.StringIO()
emodes_collector.active_users_emodes.to_csv(buffer, index=False)
client_s3.put_object(
    Body=buffer.getvalue(),
    Bucket=BUCKET,
    Key=output_path + "active_users_emodes.csv",
)

print("   --> Emodes configuration")

buffer = io.StringIO()
emodes_collector.emodes_caracteristics.to_csv(buffer, index=False)
client_s3.put_object(
    Body=buffer.getvalue(),
    Bucket=BUCKET,
    Key=output_path + "emodes_configuration.csv",
)

print(f"   --> Outputs successfully generated at: {output_path}")

print("Done!")
