import json
import logging
import boto3
from datetime import datetime
from boto3.dynamodb.conditions import Key

# Initialize DynamoDB for the user state table
dynamodb = boto3.resource("dynamodb")
state_table = dynamodb.Table("dining-concierge-user-state")
# Setup logging for debugging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Initialize SQS client
sqs = boto3.client("sqs")
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/423623846608/DiningConciergeQueue"


def lambda_handler(event, context):
    """Main entry point for Lex Code Hook"""

    # Extract session ID for user identification
    session_id = event.get("sessionId")
    logger.info(f"Session ID: {session_id}")

    # Get intent name using Lex V2 format
    intent_name = event.get("sessionState", {}).get("intent", {}).get("name")

    # If user has typed something (any intent is triggered), treat as regular flow
    if intent_name == "GreetingIntent":
        # Check for previous search data
        previous_search = get_previous_search(session_id)

        if previous_search:
            location = previous_search.get("Location")
            cuisine = previous_search.get("Cuisine")
            email = previous_search.get("Email")

            if location and cuisine and email:
                # Send previous search parameters to SQS
                send_to_sqs(location, cuisine, None, None, email)

                return build_response(
                    f"Welcome back! I've sent new recommendations for {cuisine} restaurants in {location} to your email based on your previous search. Need anything else?",
                    intent_name,
                )

        # Default greeting if not a returning user or no previous data
        return build_response("Hi there! How can I help you today?", intent_name)

    elif intent_name == "ThankYouIntent":
        return build_response("You're welcome! Have a great day!", intent_name)

    elif intent_name == "DiningSuggestionsIntent":
        return handle_dining_suggestions(event, session_id)

    else:
        return build_response(
            "I'm not sure how to handle that request.", intent_name or "FallbackIntent"
        )


def handle_dining_suggestions(event, session_id):
    """Handles the DiningSuggestionsIntent when using a Code Hook"""

    # Ensure sessionState and intent exist before accessing slots
    intent_data = event.get("sessionState", {}).get("intent", {})

    # Prevent NoneType error by ensuring slots is always a dictionary
    slots = intent_data.get("slots") or {}

    # Extract slot values safely
    def get_slot_value(slot_name):
        slot = slots.get(slot_name)
        if slot and isinstance(slot, dict):  # Ensure slot is a dictionary
            return slot.get("value", {}).get("interpretedValue")
        return None

    location = get_slot_value("Location")
    cuisine = get_slot_value("Cuisine")
    dining_time = get_slot_value("DiningTime")
    num_people = get_slot_value("NumberOfPeople")
    email = get_slot_value("Email")

    # Identify missing slots
    missing_slots = []
    if not location:
        missing_slots.append("Location")
    if not cuisine:
        missing_slots.append("Cuisine")
    if not dining_time:
        missing_slots.append("DiningTime")
    if not num_people:
        missing_slots.append("NumberOfPeople")
    if not email:
        missing_slots.append("Email")

    mapper = {
        "Location": "location",
        "Cuisine": "cuisine type",
        "DiningTime": "dining time",
        "NumberOfPeople": "number of people",
        "Email": "email",
    }
    # Ensure session state is updated with current slot values
    updated_slots = {
        "Location": {"value": {"interpretedValue": location}} if location else None,
        "Cuisine": {"value": {"interpretedValue": cuisine}} if cuisine else None,
        "DiningTime": (
            {"value": {"interpretedValue": dining_time}} if dining_time else None
        ),
        "NumberOfPeople": (
            {"value": {"interpretedValue": num_people}} if num_people else None
        ),
        "Email": {"value": {"interpretedValue": email}} if email else None,
    }

    # If a slot is missing, prompt the user for it and update the slots
    if missing_slots:
        return build_response(
            f"Can you provide your {mapper[missing_slots[0]]}?",
            "DiningSuggestionsIntent",
            elicit_slot=missing_slots[0],
            updated_slots=updated_slots,
        )

    # All slots are filled → Save user state to DynamoDB
    save_user_state(session_id, location, cuisine, email)

    # Push to SQS queue
    send_to_sqs(location, cuisine, dining_time, num_people, email)

    # If all slots are filled, proceed with fulfillment
    return build_response(
        f"Thanks! I will send restaurant recommendations for {cuisine} in {location} to {email} shortly.",
        "DiningSuggestionsIntent",
        updated_slots=updated_slots,
    )


def get_previous_search(session_id):
    """Retrieve user's previous search data from DynamoDB"""
    if not session_id:
        return None

    try:
        response = state_table.query(
            KeyConditionExpression=Key("UserID").eq(session_id),
            ScanIndexForward=False,  # Sort by LastUpdated in descending order
            Limit=1,  # Get only the most recent record
        )

        items = response.get("Items", [])
        if items:
            return items[0]
        return None

    except Exception as e:
        logger.error(f"Error retrieving previous search: {str(e)}")
        return None


def save_user_state(session_id, location, cuisine, email):
    """Save user state to DynamoDB"""
    if not session_id:
        return

    try:
        timestamp = datetime.now().isoformat()
        state_table.put_item(
            Item={
                "UserID": session_id,
                "LastUpdated": timestamp,
                "Location": location,
                "Cuisine": cuisine,
                "Email": email,
            }
        )
        logger.info(f"Saved user state for session {session_id}")

    except Exception as e:
        logger.error(f"Error saving user state: {str(e)}")


def send_to_sqs(location, cuisine, dining_time, num_people, email):
    """Send message to SQS queue"""
    message_body = {
        "Location": location,
        "Cuisine": cuisine,
        "DiningTime": dining_time,
        "NumberOfPeople": num_people,
        "Email": email,
    }

    try:
        response = sqs.send_message(
            QueueUrl=SQS_QUEUE_URL, MessageBody=json.dumps(message_body)
        )
        logger.info(f"Message sent to SQS: {response}")
        return True

    except Exception as e:
        logger.error(f"Failed to send message to SQS: {str(e)}")
        return False


def build_response(message, intent_name, elicit_slot=None, updated_slots=None):
    """Supports ElicitSlot and ensures slot values persist across turns"""

    response = {
        "sessionState": {
            "intent": {
                "name": intent_name,
                "state": "InProgress" if elicit_slot else "Fulfilled",
                "slots": updated_slots,  # Ensure slot values are updated
            }
        },
        "messages": [{"contentType": "PlainText", "content": message}],
        "requestAttributes": {},
    }

    # If a slot is missing, ask for it instead of closing
    if elicit_slot:
        response["sessionState"]["dialogAction"] = {
            "type": "ElicitSlot",
            "slotToElicit": elicit_slot,
        }

    else:
        response["sessionState"]["dialogAction"] = {"type": "Close"}

    return response
