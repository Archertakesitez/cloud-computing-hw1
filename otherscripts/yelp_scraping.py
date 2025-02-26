import requests
import boto3
from datetime import datetime
import time
from decimal import Decimal

# Yelp API Configuration
YELP_API_KEY = "blOgdbi8nb49uJMH24id6eUHv9eI3hR8kXILkSzmfHVRfO_d5ltG0vAuhyHS6VE9Chnm_UQwiMbtlggVjJvpLi-p6zyquotO6uIWitlC5LdISVwogLmf0ftn0cy3Z3Yx"
YELP_ENDPOINT = "https://api.yelp.com/v3/businesses/search"
HEADERS = {"Authorization": f"Bearer {YELP_API_KEY}"}
CUISINE_TYPES = ["chinese", "italian", "mexican"]
LOCATION = "Manhattan, NY"

# DynamoDB Configuration
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("yelp-restaurants")


# Function to call Yelp API
def get_restaurants(cuisine, offset=0):
    params = {
        "term": cuisine + " restaurants",
        "location": LOCATION,
        "limit": 50,
        "offset": offset,
    }
    response = requests.get(YELP_ENDPOINT, headers=HEADERS, params=params)
    return response.json()


# Function to store restaurant data in DynamoDB
def store_in_dynamodb(item):
    try:
        # Adding timestamp
        item["insertedAtTimestamp"] = datetime.now().isoformat()
        # Putting item in DynamoDB
        table.put_item(Item=item)
    except Exception as e:
        print("Error storing item in DynamoDB:", e)


# Function to process and filter relevant fields
def process_restaurant_data(data, cuisine):
    for business in data.get("businesses", []):
        item = {
            "restaurant_id": business.get("id"),
            "name": business.get("name"),
            "address": " ".join(business["location"].get("display_address", [])),
            "coordinates": {
                "latitude": (
                    Decimal(str(business["coordinates"]["latitude"]))
                    if business["coordinates"].get("latitude")
                    else None
                ),
                "longitude": (
                    Decimal(str(business["coordinates"]["longitude"]))
                    if business["coordinates"].get("longitude")
                    else None
                ),
            },
            "review_count": int(
                business.get("review_count", 0)
            ),  # Convert to int if needed
            "rating": Decimal(str(business.get("rating", 0))),  # Convert to Decimal
            "zip_code": business["location"].get("zip_code"),
            "cuisine_type": cuisine,
        }
        # Check if the item already exists to avoid duplicates
        existing_item = table.get_item(Key={"restaurant_id": item["restaurant_id"]})
        if "Item" not in existing_item:
            store_in_dynamodb(item)


# Main Function to Collect and Store Data
def collect_and_store_data():
    for cuisine in CUISINE_TYPES:
        print(f"Collecting data for cuisine: {cuisine}")
        # Only one request per cuisine since limit is 50 and we only need 50
        data = get_restaurants(cuisine, offset=0)
        process_restaurant_data(data, cuisine)
        time.sleep(1)  # Respect Yelp API rate limits


if __name__ == "__main__":
    collect_and_store_data()
