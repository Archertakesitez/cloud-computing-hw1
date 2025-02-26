import json
import boto3
import requests
from requests_aws4auth import AWS4Auth

# AWS Clients
sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")
ses = boto3.client("ses")
region = "us-east-1"  # Your AWS region

# Configuration
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/423623846608/DiningConciergeQueue"
OPENSEARCH_ENDPOINT = "https://search-elastic-search-restaurants-xdnt2lcwitykgu5ahg4cgmh3re.aos.us-east-1.on.aws"
DYNAMODB_TABLE = "yelp-restaurants"
SENDER_EMAIL = "ez806@nyu.edu"

# Get AWS credentials
session = boto3.Session()
credentials = session.get_credentials().get_frozen_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    region,
    "es",
    session_token=credentials.token,
)


# Get a random restaurant from OpenSearch
def get_random_restaurant(cuisine_type):
    query = {
        "size": 1,
        "query": {
            "bool": {  # Use bool query for more flexible matching
                "must": [
                    {
                        "term": {  # Exact match for keyword field
                            "cuisine_type": cuisine_type.lower()
                        }
                    }
                ]
            }
        },
    }
    try:
        print(f"Searching for cuisine: {cuisine_type}")  # Debug print
        # Use requests with AWS4Auth for SigV4 signing
        response = requests.post(
            f"{OPENSEARCH_ENDPOINT}/restaurants/_search",
            auth=awsauth,  # SigV4 Authentication
            headers={"Content-Type": "application/json"},
            data=json.dumps(query),
        )

        print(f"OpenSearch Response Status: {response.status_code}")  # Debug status
        print(f"OpenSearch Response: {response.text}")  # Print full response

        if response.status_code == 200:
            data = response.json()
            print(f"Parsed Data: {data}")  # Debug parsed data
            hits = data.get("hits", {}).get("hits", [])
            if hits:
                return hits[0]["_source"]
            else:
                print("No hits found in the response")
    except Exception as e:
        print(f"Detailed Error querying OpenSearch: {e}")
    return None


# Get restaurant details from DynamoDB
def get_restaurant_details(restaurant_id):
    table = dynamodb.Table(DYNAMODB_TABLE)
    try:
        response = table.get_item(Key={"restaurant_id": restaurant_id})
        return response.get("Item", None)
    except Exception as e:
        print(f"Error fetching from DynamoDB: {e}")
    return None


# Send email using SES
def send_email(recipient, subject, body):
    try:
        response = ses.send_email(
            Source=SENDER_EMAIL,
            Destination={"ToAddresses": [recipient]},
            Message={"Subject": {"Data": subject}, "Body": {"Text": {"Data": body}}},
        )
        print(f"Email sent! Message ID: {response['MessageId']}")
    except Exception as e:
        print(f"Error sending email: {e}")


def lambda_handler(event, context):
    # Pull message from SQS
    response = sqs.receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=1)
    messages = response.get("Messages", [])
    if not messages:
        print("No messages in the queue.")
        return

    # Get cuisine and recipient email from message
    message = messages[0]
    receipt_handle = message["ReceiptHandle"]
    body = json.loads(message["Body"])
    cuisine_type = body.get("Cuisine")  # Matches SQS structure
    recipient_email = body.get("Email")  # Matches SQS structure

    # Optional: Extract other fields if needed
    location = body.get("Location")
    dining_time = body.get("DiningTime")
    number_of_people = body.get("NumberOfPeople")

    # Debug Logging
    print(f"Message received: {body}")
    print(f"Cuisine: {cuisine_type}, Email: {recipient_email}")
    print(
        f"Location: {location}, DiningTime: {dining_time}, NumberOfPeople: {number_of_people}"
    )

    # Check for required fields
    if not cuisine_type or not recipient_email:
        print("Invalid message format.")
        return

    # Get random restaurant from OpenSearch
    restaurant = get_random_restaurant(cuisine_type)
    if not restaurant:
        print("No restaurant found.")
        return

    # Get more details from DynamoDB
    restaurant_id = restaurant["restaurant_id"]
    details = get_restaurant_details(restaurant_id)
    if not details:
        print("No details found in DynamoDB.")
        return

    # Format and send email
    subject = f"Recommended {cuisine_type} Restaurant"
    body = (
        f"Check out this {cuisine_type} restaurant in {location}:\n"
        f"Name: {details['name']}\n"
        f"Address: {details['address']}\n"
        f"Dining Time: {dining_time}\n"
        f"Number of People: {number_of_people}"
    )
    send_email(recipient_email, subject, body)

    # Delete processed message only after successful email
    if recipient_email:
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
    print("Message processed and deleted.")
