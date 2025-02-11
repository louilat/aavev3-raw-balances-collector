"""ETL for extracting users balances"""

import boto3
import os
import io
import pandas as pd
from datetime import datetime, timedelta
import json
from web3 import Web3
from src.balances_collector.balances_collector import AaveV3RawBalancesCollector
from src.treasury.reserves_treasury import collect_reserves_treasury

# Run parameters
output_path = None
users_list_input_path = None
block_number = None

if users_list_input_path is None:
    block_number = "latest"
    yesterday = datetime.today() - timedelta(days=1)
    snapshot_date = yesterday.strftime("%Y-%m-%d")
    users_list_input_path = f"aave-raw-datasource/daily-decoded-events/decoded_events_snapshot_date={snapshot_date}/all_active_users.csv"
    output_path = f"aave-raw-datasource/daily-users-balances/users_balances_snapshot_date={snapshot_date}/"


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


print("STEP 0: Extracting and collecting data...")

print("   --> Extracting UIPoolDataProvider abi...")
with open("./src/abi/ui_pool_data_provider.json") as file:
    data_provider_abi = json.load(file)

print("   --> Extracting users list...")
object = client_s3.get_object(Bucket=BUCKET, Key=users_list_input_path)
users_data = pd.read_csv(object["Body"])

print("STEP 1: Collecting raw users balances...")

collector = AaveV3RawBalancesCollector(
    w3=w3,
    contract_abi=data_provider_abi,
    block_number=block_number,
)

collector.collect_raw_balances(users_data)

print("STEP 2: Collecting reserves data...")

collector.collect_reserves_data()

print("STEP 3: Processing users balances...")

collector.process_raw_balances()

print("STEP 4: Collecting and matching reserves treasury with reserves data...")

reserves_data = collect_reserves_treasury(
    w3=w3, reserves_data=collector.reserves_data, block_number=block_number
)

print("STEP 4: Uploading outputs to s3...")

buffer = io.StringIO()
collector.processed_balances.to_csv(buffer, index=False)
client_s3.put_object(
    Body=buffer.getvalue(),
    Bucket=BUCKET,
    Key=output_path + "active_users_balances.csv",
)

buffer = io.StringIO()
reserves_data.to_csv(buffer, index=False)
client_s3.put_object(
    Body=buffer.getvalue(), Bucket=BUCKET, Key=output_path + "reserves_data.csv"
)

print(f"   --> Outputs successfully generated at: {output_path}")

print("Done!")
