import csv

import json

import os

tablename = "lookup-dynamo-table-lookupdynamotablelookupdynamotableB82CF843-1LAU3UKQVOMLN"
csv_file = 'si_delete_event_epoch__xaa.csv'

json_list = []
json_file_counter = 1
max_items_per_file = 2000

with open(csv_file, mode='r') as file:
    csv_reader = csv.DictReader(file)

    for row in csv_reader:
        json_dict = {"id": row["id"], "sort_key": row["sort_key"]}
        json_list.append(json_dict)

        # Check if the list has reached the maximum items per file
        if len(json_list) >= max_items_per_file:
            json_data = {"table_name": tablename, "item_list": json_list}
            # JSON output filename
            json_output_file = f'result/chunk_1/output_{json_file_counter}.json'

            # Write the payload dict to the JSON file
            with open(json_output_file, mode='w') as output_file:
                json.dump(json_data, output_file, indent=4)

            print(f'Data written to {json_output_file}', len(json_list))
            json_file_size = os.path.getsize(json_output_file)
            file_size_mb = json_file_size / (1024 * 1024)
            print(f"size of {json_output_file} {file_size_mb} mb ")

            # Reset the list and increment the file counter
            json_list = []
            json_file_counter += 1

# Check if there are remaining items in the list after processing
if json_list:
    json_data = {"table_name": tablename, "item_list": json_list}
    json_output_file = f'result/chunk_1/output_{json_file_counter}.json'

    with open(json_output_file, mode='w') as output_file:
        json.dump(json_data, output_file, indent=4)

    print(f'Data written to {json_output_file}', len(json_list))
    json_file_size = os.path.getsize(json_output_file)
    file_size_mb = json_file_size / (1024 * 1024)
    print(f"size of {json_output_file} {file_size_mb} mb ")
