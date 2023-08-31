from azure.cosmos import CosmosClient, PartitionKey

### DON'T USE ####

# Cosmos DB settings
endpoint_uri = "https://cosmos-db-szos.documents.azure.com:443/"
primary_key = "VN09p2xxIfzgNNWb5I6ilQofIeMAvuCj27NbHHFAkNYCXKSpMvCLrssgAi9dt5FJrqSWwdZykSnyACDbdnICNA=="
database_name = "cfb"
container_name = "Schedule"

# Create Cosmos DB client
client = CosmosClient(endpoint_uri, primary_key)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)

# The ID of the document you want to update
document_id = "13667"

# SQL query to update the document
query = f"SELECT * FROM c WHERE c.id = '{document_id}'"

# Retrieve the existing document
existing_item = container.query_items(query=query, enable_cross_partition_query=True)

for item in existing_item:
    item["Period"] = "12222"  # Replace with the property you want to update
    
    # Replace the existing document with the updated one
    container.replace_item(item=item, body=item)
    print(f"Document with ID {document_id} updated successfully.")
