"""ETL for extracting users balances"""

import boto3
import os
import io
import pandas as pd
from datetime import datetime, timedelta
import json
from src.balances_collector.balances_collector import AaveV3RawBalancesCollector

# Run parameters
output_path = None
users_list_input_path = None

if users_list_input_path is None:
    yesterday = datetime.today() - timedelta(days=1)
    snapshot_date = yesterday.strftime("%Y-%m-%d")
    users_list_input_path = f"aave-events-collector/daily-decoded-events/decoded_events_snapshot_date={snapshot_date}/all_active_users.csv"
    output_path = f"aave-events-collector/daily-users-balances/users_balances_snapshot_date={snapshot_date}/"


AWS_ACCESS_KEY = os.environ["AWS_ACCESS_KEY"]
AWS_SECRET_KEY = os.environ["AWS_SECRET_KEY"]
PROVIDER_URL = os.environ["PROVIDER_URL"]

client_s3 = boto3.client(
    "s3",
    endpoint_url = 'https://'+'minio.lab.sspcloud.fr',
    aws_access_key_id= AWS_ACCESS_KEY, 
    aws_secret_access_key= AWS_SECRET_KEY, 
)

print("STEP 0: Extracting data...")

print("   --> Extracting UIPoolDataProvider abi...")

with open("./src/abi/ui_pool_data_provider.json") as file:
    data_provider_abi = json.load(file)

print("   --> Extracting users list...")

object = client_s3.get_object(Bucket="llatournerie", Key=users_list_input_path)
users_data = pd.read_csv(object["Body"])

print("STEP 1: Collecting raw users balances...")

collector = AaveV3RawBalancesCollector(
    provider_url=PROVIDER_URL,
    contract_abi=data_provider_abi,
)

collector.collect_raw_balances(users_data)

print("STEP 2: Collecting reserves data...")

collector.collect_reserves_data()

print("STEP 3: Uploading outputs to s3...")

buffer = io.StringIO()
collector.all_users_balances.to_csv(buffer, index=False)
client_s3.put_object(Body=buffer.getvalue(), Bucket="llatournerie", Key=output_path + "active_users_balances.csv")

buffer = io.StringIO()
collector.reserves_data.to_csv(buffer, index=False)
client_s3.put_object(Body=buffer.getvalue(), Bucket="llatournerie", Key=output_path + "reserves_data.csv")

print(f"   --> Outputs successfully generated at: {output_path}")

print("Done!")
