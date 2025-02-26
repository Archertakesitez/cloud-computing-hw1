import boto3
import json

# Define your table name and the two columns you want
TABLE_NAME = "yelp-restaurants"
COLUMNS = ["restaurant_id", "cuisine_type"]

# Initialize DynamoDB resource
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)

# Scan the table and get all items
response = table.scan()
items = response["Items"]

# Extract only the two columns
filtered_data = []
for item in items:
    filtered_item = {col: item.get(col, None) for col in COLUMNS}
    filtered_data.append(filtered_item)

# Save the extracted data as JSON
output_file = "restaurants_partial.json"
with open(output_file, "w") as json_file:
    json.dump(filtered_data, json_file, indent=4)

print(f"JSON file created: {output_file}")
