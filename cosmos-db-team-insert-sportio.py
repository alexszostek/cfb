import http.client
import json
import uuid
from azure.cosmos import CosmosClient, exceptions

# Cosmos DB settings
endpoint_uri = "https://cosmos-db-szos.documents.azure.com:443/"
primary_key = "VN09p2xxIfzgNNWb5I6ilQofIeMAvuCj27NbHHFAkNYCXKSpMvCLrssgAi9dt5FJrqSWwdZykSnyACDbdnICNA=="
database_name = "cfb"
container_name = "Teams"

# API request to get data
conn = http.client.HTTPSConnection("api.sportsdata.io")
payload = ''
headers = {}
conn.request("GET", "/v3/cfb/scores/json/TeamsBasic?key=257077aa8bbe453ab5270f8a471be129", payload, headers)
res = conn.getresponse()
data = res.read()

# Decode the JSON response
decoded_data = data.decode("utf-8")

# Parse JSON data
teams_data = json.loads(decoded_data)

# Create Cosmos DB client
client = CosmosClient(endpoint_uri, primary_key)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)

# Insert each record as a new document into Cosmos DB
for record in teams_data:
    # Generate a GUID for the "id" field
    record["id"] = str(uuid.uuid4())  # Generate a new UUID and convert it to string
    
    # Insert document into Cosmos DB
    try:
        response = container.create_item(body=record)
        print(f"Inserted document with ID: {response['id']}")
    except exceptions.CosmosHttpResponseError as e:
        print(f"An error occurred: {e}")

