import pandas as pd
from azure.cosmos import CosmosClient
import datetime
import os

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
df_team_away = query_cosmos_to_dataframe(teams_container, teams_query)
df_team_away.columns = [f"{col}_away" for col in df_team_away.columns]

# Query for home teams
df_team_home = query_cosmos_to_dataframe(teams_container, teams_query)
df_team_home.columns = [f"{col}_home" for col in df_team_home.columns]

# Merge and select columns for away teams
merged_df = pd.merge(df_schedule, df_team_away, left_on="AwayTeamID", right_on="TeamID_away", how="left")
selected_columns_df = merged_df[["Week", "AwayTeamName", "AwayTeamID", "HomeTeamName", "HomeTeamID", "PointSpread", "OverUnder", "DateTimeUTC", "NeutralVenue", "Conference_away"]]

# Merge and select columns for home teams
selected_columns_df = pd.merge(selected_columns_df, df_team_home, left_on="HomeTeamID", right_on="TeamID_home", how="left")
selected_columns_df = selected_columns_df[["Week", "AwayTeamName", "AwayTeamID", "HomeTeamName", "HomeTeamID", "PointSpread", "OverUnder", "DateTimeUTC", "NeutralVenue", "Conference_away", "Conference_home"]]

# Add a new column for team concatenation
selected_columns_df['TeamsConcatenated'] = selected_columns_df['AwayTeamName'] + ' vs ' + selected_columns_df['HomeTeamName']

# Add a new column for team concatenation
selected_columns_df['ConferenceConcatenated'] = selected_columns_df['Conference_away'] + ' vs ' + selected_columns_df['Conference_home']

# Generate the current date
current_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Define the folder name
folder_name = "schedule"

# Create the folder if it doesn't exist
if not os.path.exists(folder_name):
    os.makedirs(folder_name)

# Define the list of partial team names you want to filter for
partial_team_names = ["Michigan Wolver", "Michigan State", "Rutgers", "North Carolina Tar", "Illinois Fight" , "North Texas","LSU", "Purdue", "Houston Coug", "Minnesota Gold", "Florida Ga", "Tulane"]
week_number = 1  # The week number you want to filter for

# Filter based on partial team names and week number
selected_columns_df = selected_columns_df[(selected_columns_df['AwayTeamName'].str.contains('|'.join(partial_team_names)) |
                                           selected_columns_df['HomeTeamName'].str.contains('|'.join(partial_team_names))) &
                                          (selected_columns_df['Week'] == week_number)]

# ... (other code remains the same)
print(selected_columns_df)

# Create the new CSV file path with folder and date appended
csv_filepath = f"{folder_name}/scheduled_results_{current_datetime}_week_{week_number}.csv"

# Save the selected results to the new CSV file
selected_columns_df.to_csv(csv_filepath, index=False)
print(f"Selected results saved to '{csv_filepath}'")