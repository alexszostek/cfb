import pandas as pd
from azure.cosmos import CosmosClient
import datetime

# Cosmos DB settings
ENDPOINT_URI = "https://cosmos-db-szos.documents.azure.com:443/"
PRIMARY_KEY = "VN09p2xxIfzgNNWb5I6ilQofIeMAvuCj27NbHHFAkNYCXKSpMvCLrssgAi9dt5FJrqSWwdZykSnyACDbdnICNA=="
DATABASE_NAME = "cfb"
SCHEDULE_CONTAINER_NAME = "Schedule"
TEAMS_CONTAINER_NAME = "Teams"

# Create Cosmos DB client
client = CosmosClient(ENDPOINT_URI, PRIMARY_KEY)
database = client.get_database_client(DATABASE_NAME)

# Function to query Cosmos DB and return a DataFrame
def query_cosmos_to_dataframe(container, query):
    result_iterable = container.query_items(query=query, enable_cross_partition_query=True)
    data = list(result_iterable)
    return pd.DataFrame(data)

# Query for schedule results
schedule_container = database.get_container_client(SCHEDULE_CONTAINER_NAME)
schedule_query = "SELECT c.Week, c.AwayTeamName, c.AwayTeamID, c.HomeTeamName, c.HomeTeamID, c.PointSpread, c.OverUnder, c.DateTimeUTC, c.NeutralVenue FROM c"
df_schedule = query_cosmos_to_dataframe(schedule_container, schedule_query)

# Query for teams results
teams_container = database.get_container_client(TEAMS_CONTAINER_NAME)
teams_query = "SELECT * FROM c"

# Query for away teams
df_team = query_cosmos_to_dataframe(teams_container, teams_query)

# Generate the current date
current_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Create the new CSV file name with date appended
csv_filename = f"teams_{current_datetime}.csv"

# Save the selected results to the new CSV file
df_team.to_csv(csv_filename, index=False)
print(f"Selected results saved to '{csv_filename}'")



