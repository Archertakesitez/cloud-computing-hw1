from opensearchpy import OpenSearch, RequestsHttpConnection
import boto3
from requests_aws4auth import AWS4Auth

# AWS credentials
region = "us-east-1"
service = "es"  # Changed from 'es' to 'aoss'

# Get credentials from the default profile
session = boto3.Session()
credentials = session.get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    region,
    service,
    session_token=(
        credentials.token if credentials.token else None
    ),  # Handle token conditionally
)

# OpenSearch client
host = (
    "search-elastic-search-restaurants-xdnt2lcwitykgu5ahg4cgmh3re.aos.us-east-1.on.aws"
)
client = OpenSearch(
    hosts=[{"host": host, "port": 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
)

# Read and upload the bulk file
with open("restaurants_bulk.json", "r") as f:
    bulk_data = f.read()
    response = client.bulk(body=bulk_data)

# Print response to verify
print(response)

# Verify upload
try:
    search_response = client.search(
        index="restaurants", body={"query": {"match_all": {}}, "size": 1}
    )
    print(
        "\nUpload verification - sample document:", search_response["hits"]["hits"][0]
    )
except Exception as e:
    print("Error checking index:", e)
