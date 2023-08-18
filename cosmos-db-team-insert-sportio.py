import http.client
import json
from azure.cosmos import CosmosClient, exceptions

# Cosmos DB settings
endpoint_uri = "https://cosmos-db-szos.documents.azure.com:443/"
primary_key = "VN09p2xxIfzgNNWb5I6ilQofIeMAvuCj27NbHHFAkNYCXKSpMvCLrssgAi9dt5FJrqSWwdZykSnyACDbdnICNA=="
database_name = "cfb"
container_name = "Teams"

# API request to get data
conn = http.client.HTTPSConnection("api.sportsdata.io")
conn.request("GET", "/v3/cfb/scores/json/TeamsBasic?key=257077aa8bbe453ab5270f8a471be129")
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

def insert_or_replace_document(container, record_id, record):
    try:
        container.replace_item(item=record_id, body=record)
        print(f"Replaced document with ID: {record_id}")
    except exceptions.CosmosResourceNotFoundError:
        container.create_item(body=record)
        print(f"Inserted document with ID: {record_id}")
    except exceptions.CosmosHttpResponseError as e:
        print(f"An error occurred: {e}")

# Insert each record as a new document into Cosmos DB
for record in teams_data:
    record["id"] = str(record["School"]) # Generate a new UUID and convert it to string
    record_id = str(record["School"])  # Use the school name as the document ID
    
    # SQL query to check if the document exists
    query = f"SELECT VALUE COUNT(1) FROM c WHERE c.id = '{record_id}'"
    count_query_result = container.query_items(query=query, enable_cross_partition_query=True)
    record_count = next(count_query_result)
    
    if record_count > 0:
        insert_or_replace_document(container, record_id, record)
    else:
        insert_or_replace_document(container, record_id, record)
