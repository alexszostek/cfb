import pandas as pd
from azure.cosmos import CosmosClient
import datetime
import os
import json

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
schedule_query = "SELECT c.Week, c.AwayTeamName, c.AwayTeamID, c.HomeTeamName, c.HomeTeamID, c.PointSpread, c.OverUnder, c.DateTime, c.NeutralVenue FROM c"
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
selected_columns_df = merged_df[["Week", "AwayTeamName", "AwayTeamID", "HomeTeamName", "HomeTeamID", "PointSpread", "OverUnder", "DateTime", "NeutralVenue", "Conference_away"]]

# Merge and select columns for home teams
selected_columns_df = pd.merge(selected_columns_df, df_team_home, left_on="HomeTeamID", right_on="TeamID_home", how="left")
selected_columns_df = selected_columns_df[["Week", "AwayTeamName", "HomeTeamName", "PointSpread", "OverUnder", "DateTime", "NeutralVenue", "Conference_away", "Conference_home"]]

# Generate the current date
current_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Define the folder name
folder_name = "schedule"

# Create the folder if it doesn't exist
if not os.path.exists(folder_name):
    os.makedirs(folder_name)

# Define the path to your JSON file
json_file_path = r"C:\temp\Github\cfb\weekly-matchups.json"

# Read the JSON file containing partial team names and week number
with open(json_file_path, "r") as json_file:
    team_data = json.load(json_file)

partial_team_names = team_data["partial_team_names"]
week_number = team_data["week_number"]
    
# Convert partial team names to lowercase for case-insensitive matching
partial_team_names_lower = [name.lower() for name in partial_team_names]

# Convert team names in the DataFrame to lowercase for case-insensitive matching
selected_columns_df['AwayTeamName_lower'] = selected_columns_df['AwayTeamName'].str.lower()
selected_columns_df['HomeTeamName_lower'] = selected_columns_df['HomeTeamName'].str.lower()

# Filter based on partial team names and week number (case-insensitive)
selected_columns_df = selected_columns_df[(selected_columns_df['AwayTeamName_lower'].str.contains('|'.join(partial_team_names_lower)) |
                                           selected_columns_df['HomeTeamName_lower'].str.contains('|'.join(partial_team_names_lower))) &
                                          (selected_columns_df['Week'] == week_number)]

# Drop the lowercase columns used for matching if not needed in the final output
selected_columns_df = selected_columns_df.drop(columns=['AwayTeamName_lower', 'HomeTeamName_lower'])

# Create the new CSV file path with folder and date appended
csv_filepath = f"{folder_name}/scheduled_results_{current_datetime}_week_{week_number}.csv"

# Save the selected results to the new CSV file
selected_columns_df.to_csv(csv_filepath, index=False)
print(f"Selected results saved to '{csv_filepath}'")


# Add a "row number" column starting from 1
selected_columns_df['Row Number'] = range(1, len(selected_columns_df) + 1)

# Print the DataFrame with row numbers
print(selected_columns_df)