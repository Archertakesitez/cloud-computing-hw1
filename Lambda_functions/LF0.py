import json
import boto3
import hashlib

def get_session(event):
    # Extract headers for user identification 
    headers = event.get('headers', {})
    user_agent = headers.get('User-Agent', '')
    source_ip = headers.get('X-Forwarded-For', '').split(',')[0].strip()

    print("user_agent: ", user_agent, " source_ip: ", source_ip)
    # Generate a unique user ID
    user_id = hashlib.md5((user_agent + source_ip).encode()).hexdigest()

    # Create a session ID that persists the user ID
    session_id = f"user-{user_id}"
    print("session_id: ", session_id)
    return session_id

def lambda_handler(event, context):
    print("received event: ", event)

    # Assume the role in your account
    sts_client = boto3.client('sts')
    assumed_role_object = sts_client.assume_role(
        RoleArn="arn:aws:iam::423623846608:role/CrossAccountTester",
        RoleSessionName="LexCrossAccountSession"
    )
    
    # Get the credentials
    credentials = assumed_role_object['Credentials']
    
    # Create Lex client with assumed role credentials
    lex_client = boto3.client(
        'lexv2-runtime',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
        region_name='us-east-1'
    )
    
    # grab the session id from the frontend request
    # session_id = event['session_id']
    session_id = get_session(event)

    # and the message
    message = event['messages'][0]['unstructured']['text']
    print("session_id: ", session_id, " message: ", message)

    # Now make the Lex API call
    response = lex_client.recognize_text(
        botId='LWS6DL48KC',
        botAliasId='W9S1WSGFXJ',
        localeId='en_US',
        sessionId=session_id, 
        text=message,  # The text to send to the bot
    )
    print("lex response: ", response)

    if 'messages' not in response:
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'messages': [{'type': 'unstructured', 'unstructured':{'text':'Sorry, I don\'t understand.'}}]
        }
    msg = response['messages'][0]['content']

    return {
        # reformat the data for the frontend
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
        # currently expects only 1 message per response
        'messages': [{'type': 'unstructured', 'unstructured':{'text':msg}}]
    }
