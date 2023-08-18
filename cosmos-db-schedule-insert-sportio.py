import http.client
import json
from azure.cosmos import CosmosClient, exceptions

# Cosmos DB settings
endpoint_uri = "https://cosmos-db-szos.documents.azure.com:443/"
primary_key = "VN09p2xxIfzgNNWb5I6ilQofIeMAvuCj27NbHHFAkNYCXKSpMvCLrssgAi9dt5FJrqSWwdZykSnyACDbdnICNA=="
database_name = "cfb"
container_name = "Schedule"

# Create Cosmos DB client
client = CosmosClient(endpoint_uri, primary_key)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)

# Loop through weeks 1 to 13
for week_number in range(1, 14):
    # print(week_number)
    # API request to get data for the week
    conn = http.client.HTTPSConnection("api.sportsdata.io")
    payload = ''
    headers = {}
    conn.request("GET", f"/v3/cfb/scores/json/GamesByWeek/2023/{week_number}?key=257077aa8bbe453ab5270f8a471be129", payload, headers)
    res = conn.getresponse()
    data = res.read()

    
    # Decode the JSON response
    decoded_data = data.decode("utf-8")
    
    # Parse JSON data
    games_data = json.loads(decoded_data)

    # Insert or replace each record as a new document into Cosmos DB
    for record in games_data:
        # Use GameID as the "id" field
        record["id"] = str(record["GameID"])
        record_id = str(record["GameID"])
        
        # SQL query to update the document
        query = f"SELECT * FROM c WHERE c.id = '{record_id}'"
        count_query = f"SELECT VALUE COUNT(1) from c where c.id = '{record_id}'"
              
        result_iterable = container.query_items(query=count_query, enable_cross_partition_query=True)
        
        first_item = next(result_iterable)
            
        existing_item = container.query_items(query=query, enable_cross_partition_query=True)
            
        # Insert or replace document into Cosmos DB
        try:
            if first_item > 0:
                container.replace_item(item=record_id, body=record)
                print(f"Replaced document with ID: {record_id}")
            else:
                container.create_item(body=record)
                print(f"Inserted document with ID: {record_id}")
        except exceptions.CosmosHttpResponseError as e:
            print(f"An terror occurred: {e}")
