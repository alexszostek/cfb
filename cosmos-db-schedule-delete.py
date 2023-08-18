from azure.cosmos import CosmosClient

# Cosmos DB settings
endpoint_uri = "https://cosmos-db-szos.documents.azure.com:443/"
primary_key = "VN09p2xxIfzgNNWb5I6ilQofIeMAvuCj27NbHHFAkNYCXKSpMvCLrssgAi9dt5FJrqSWwdZykSnyACDbdnICNA=="
database_name = "cfb"
container_name = "Schedule"

# Create Cosmos DB client
client = CosmosClient(endpoint_uri, primary_key)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)

# Get all documents in the container
query = "SELECT * FROM c"
query_results = container.query_items(query=query, enable_cross_partition_query=True)


print(query_results)

# # Delete each document
# for item in query_results:
#     container.delete_item(item, item['id'])  # Delete the item by providing its ID
