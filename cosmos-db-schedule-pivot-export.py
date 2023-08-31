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

# Query for away teams with selected columns
away_teams_query = "SELECT c.Week, c.AwayTeamName, c.AwayTeamID FROM c"
df_team_away = query_cosmos_to_dataframe(schedule_container, away_teams_query)
df_team_away = df_team_away.rename(columns={'AwayTeamName': 'TeamName', 'AwayTeamID': 'TeamID'})

# Query for home teams with selected columns
home_teams_query = "SELECT c.Week, c.HomeTeamName, c.HomeTeamID FROM c"
df_team_home = query_cosmos_to_dataframe(schedule_container, home_teams_query)
df_team_home = df_team_home.rename(columns={'HomeTeamName': 'TeamName', 'HomeTeamID': 'TeamID'})

# Concatenate the two dataframes vertically to create a union
df_combined_teams = pd.concat([df_team_away, df_team_home], ignore_index=True)

# Generate the current date
current_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Create the new CSV file name with date appended
csv_filename = f"scheduled_pivot_{current_datetime}.csv"

# Save the selected results to the new CSV file
df_combined_teams.to_csv(csv_filename, index=False)
print(f"Selected results saved to '{csv_filename}'")
