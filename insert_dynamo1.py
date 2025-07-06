import boto3
import random
import string
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- Configurable Parameters ---
TABLE_NAME = "MyTable"
TOTAL_RECORDS = 10_000
GROUP_SIZE = 1000               # Logical group
DDB_BATCH_LIMIT = 25            # AWS max per batch write
THREADS = 5                     # Parallel writers

# --- Initialize DynamoDB ---
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)

# --- Generate unique 10-char alphanumeric ID ---
def generate_unique_ids(count, length=10):
    seen = set()
    while len(seen) < count:
        uid = ''.join(random.choices(string.ascii_letters + string.digits, k=length))
        if uid not in seen:
            seen.add(uid)
            yield uid

# --- Generate random 10-char offer code ---
def generate_offer_code(length=10):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

# --- TTL timestamp for 24 hours later ---
def get_ttl_timestamp():
    return int((datetime.utcnow() + timedelta(days=1)).timestamp())

# --- Write a group of 1000 records in internal 25-item chunks ---
def write_group(records):
    for i in range(0, len(records), DDB_BATCH_LIMIT):
        batch = records[i:i + DDB_BATCH_LIMIT]
        with table.batch_writer(overwrite_by_pkeys=["id"]) as writer:
            for item in batch:
                writer.put_item(Item=item)

# --- Main record generator grouped in 1000s ---
def generate_groups():
    ttl = get_ttl_timestamp()
    unique_ids = generate_unique_ids(TOTAL_RECORDS)
    group = []
    for i, uid in enumerate(unique_ids, 1):
        group.append({
            "id": uid,
            "elmooffercode": generate_offer_code(),
            "ttl": ttl
        })
        if i % GROUP_SIZE == 0:
            yield group
            group = []
    if group:
        yield group

# --- Main Execution ---
if __name__ == "__main__":
    print(f"Inserting {TOTAL_RECORDS} records into DynamoDB table '{TABLE_NAME}'...")
    start_time = time.time()

    groups = list(generate_groups())
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        executor.map(write_group, groups)

    print(f"✅ Done: Inserted {TOTAL_RECORDS} items in {time.time() - start_time:.2f} seconds.")
