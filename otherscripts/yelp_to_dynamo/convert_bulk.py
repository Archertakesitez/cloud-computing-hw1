import json


def convert_to_bulk_format(input_file, output_file):
    # Read the input JSON file
    with open(input_file, "r") as f:
        input_data = json.load(f)

    bulk_lines = []
    for item in input_data:
        # Add the index action line
        action = {"index": {"_index": "restaurants", "_id": item["restaurant_id"]}}
        bulk_lines.append(json.dumps(action))

        # Add the document line
        document = {
            "restaurant_id": item["restaurant_id"],
            "cuisine_type": item["cuisine_type"].lower(),
        }
        bulk_lines.append(json.dumps(document))

    # Join with newlines and add final newline
    bulk_format = "\n".join(bulk_lines) + "\n"

    # Write to output file
    with open(output_file, "w") as f:
        f.write(bulk_format)

    print(f"Conversion complete! Bulk data saved to {output_file}")
    print("First few lines of the converted format:")
    print("\n".join(bulk_lines[:4]))  # Show first 2 documents


# Convert the file
convert_to_bulk_format("restaurants_partial.json", "restaurants_bulk.json")
