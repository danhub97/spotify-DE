import spotipy
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime
import pandas as pd
import os
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas_gbq

# Load environment variables from .env
load_dotenv() 

# load credentials
SPOTIPY_CLIENT_ID = os.getenv('MY_APP_CLIENT_ID')
SPOTIPY_CLIENT_SECRET = os.getenv('MY_APP_SECRET_KEY')
SPOTIPY_REDIRECT_URI = 'https://www.danhubbell.co.uk/'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./gcp_spotify_project_credentials.json"

# BigQuery settings
project_id = "spotify-project-435021"
dataset_id = "spotify_data"

# Create a BigQuery client
bq_client = bigquery.Client()


# Set the scope to access recently played tracks
scope = 'user-read-recently-played'

# Authenticate with Spotify
spotify_client = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=SPOTIPY_CLIENT_ID,
                                               client_secret=SPOTIPY_CLIENT_SECRET,
                                               redirect_uri=SPOTIPY_REDIRECT_URI,
                                               scope=scope))


#FUNCTIONS

def check_if_valid_data(df: pd.DataFrame) -> bool:
    """Check if dataframe is empty, or if it contains null values or duplicate listen times"""

    if df.empty:
        print("No songs listened to. Finishing execution.")
        return False

    if pd.series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key Check violated: Duplicate Played At values")
    
    if df.isnull().values.any():
        raise Exception("Null Values.")

def get_all_recently_played_tracks(sp, limit=50):
    all_tracks = []
    results = sp.current_user_recently_played(limit=limit)
    all_tracks.extend(results['items'])

    return all_tracks


def ingest_dimension(table_name, dataset_name, dataframe, id_column):
    """Ingests data into a BigQuery table, only ingesting new data. If no existing data, ingests all data"""
    try:
        #Get df of ids for existing data
        query = f"SELECT {id_column} FROM {dataset_name}.{table_name}"
        existing_data = pandas_gbq.read_gbq(query, project_id)

        #If no existing data, ingest all data
        if existing_data.empty:
            new_data = dataframe
            print(f"No existing data in {table_name}, ingesting all data")
        #If existing data, only ingest new data
        else:
            new_data = dataframe[~dataframe[id_column].isin(existing_data[id_column])]

        if new_data.empty:
            print(f"No new data to ingest into {table_name}")
            return

        #Drop duplicates
        new_data.drop_duplicates(inplace=True)
        #Ingest new data into table

        table_id = f"{project_id}.{dataset_id}.{table_name}"

        pandas_gbq.to_gbq(new_data,table_id, project_id=project_id, if_exists='append')
        print(f"Successfully ingested {len(new_data)} rows into {table_name}")

    except pd.io.gbq.GenericGBQException as e:
        print(f"Error ingesting data into {table_name}: {e}")
    except Exception as e:
        print(f"Error ingesting data into {table_name}: {e}")



def get_most_recent_play_time():
    """Get the most recent play time from the play table"""

    query_job = pandas_gbq.read_gbq('select max(play_time) from `spotify_data.play`', project_id=project_id)
    # If no listens in table, returns None
    most_recent_play_time = query_job['f0_'][0]
    return most_recent_play_time


def ingest_plays(plays_df):
    """Ingests data into the play table, only ingesting new data. If no existing data, ingests all data"""
    # Get most recent listen time
    most_recent_play_time = get_most_recent_play_time()

    # Get new listens dataframe
    if most_recent_play_time is None:
        # If no listens in table, ingest all listens
        print('No listens in play table, ingesting all listens')
        new_listens = plays_df
    else:   
        # Only keep plays with listen time after the latest in the sql table
        new_listens = plays_df[plays_df['play_time'] > most_recent_play_time]

    if new_listens.empty:
        print("No new listens to ingest")
        return
    
    # Drop duplicates
    new_listens.drop_duplicates(inplace=True)

    # Append new listens dataframe to play table
    table_id = f"{project_id}.{dataset_id}.play"
    pandas_gbq.to_gbq(new_listens,table_id, project_id=project_id, if_exists='append')
    print(f"Successfully ingested {len(new_listens)} rows into play table")


def delete_all_tables():
    """Deletes all rows from all tables in the dataset"""

    tables = ['album', 'artist', 'track', 'play']
    for table in tables:
        try:
            query = f"DELETE FROM `{dataset_id}.{table}` WHERE 1=1"
            query_job = bq_client.query(query)
            print(f"Deleted all rows from {table}")
        except Exception as e:
            print(f"Error deleting rows from {table}: {e}")


# MAIN
            
# Get all recently played tracks via spotipy api
all_recent_tracks = get_all_recently_played_tracks(spotify_client, limit=50)

# Create dictionaries to store data
album_dict = {
    "album_spotify_id": [],
    "album_name": [],
    "album_release_date": [],
    "total_tracks": [],
    "album_url": [],
    "cover_url": [],
    "artist_spotify_id": []
}

track_dict = {
    "track_spotify_id": [],
    "track_name": [],
    "duration_ms": [],
    "explicit": [],
    "track_number": [],
    "track_url": [],
    "track_type": [],
    "album_spotify_id": []
}

play_history_dict = {
    'play_time': [],
    'track_spotify_id': [],
    'album_spotify_id': [],
    'artist_spotify_id': []
}

artist_dict = { 
    "artist_spotify_id" : [],
    "artist_name" : [] 
}

# Loop through all tracks and populate dictionaries
for track in all_recent_tracks:
    album_dict["album_spotify_id"].append(track['track']['album']['id'])
    album_dict["album_name"].append(track['track']['album']['name'])
    album_dict["album_release_date"].append(track['track']['album']['release_date'])
    album_dict["total_tracks"].append(track['track']['album']['total_tracks'])
    album_dict["album_url"].append(track['track']['album']['external_urls']['spotify'])
    album_dict["cover_url"].append(track['track']['album']['images'][0]['url'])
    album_dict["artist_spotify_id"].append(track['track']['album']['artists'][0]['id'])

    artist_dict['artist_spotify_id'].append(track['track']['album']['artists'][0]['id'])
    artist_dict['artist_name'].append(track['track']['album']['artists'][0]['name'])

    track_dict["track_spotify_id"].append(track['track']['id'])
    track_dict["track_name"].append(track['track']['name'])
    track_dict["duration_ms"].append(track['track']['duration_ms'])
    track_dict["explicit"].append(track['track']['explicit'])
    track_dict["track_number"].append(track['track']['track_number'])
    track_dict["track_url"].append(track['track']['external_urls']['spotify'])
    track_dict["track_type"].append(track['track']['type'])
    track_dict["album_spotify_id"].append(track['track']['album']['id'])

    # Convert play time to datetime object and format
    play_time = datetime.strptime(track['played_at'], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S")
    play_history_dict['play_time'].append(play_time)
    play_history_dict['track_spotify_id'].append(track['track']['id'])
    play_history_dict['album_spotify_id'].append(track['track']['album']['id'])
    play_history_dict['artist_spotify_id'].append(track['track']['album']['artists'][0]['id'])


# Create dataframes from dictionaries
albums_dataframe = pd.DataFrame(album_dict)
tracks_dataframe = pd.DataFrame(track_dict)
artists_dataframe = pd.DataFrame(artist_dict)
plays_dataframe = pd.DataFrame(play_history_dict)

# Ingest data into BigQuery
ingest_plays(plays_df=plays_dataframe)
ingest_dimension("album", 'spotify_data', albums_dataframe, "album_spotify_id")
ingest_dimension("track", 'spotify_data', tracks_dataframe, "track_spotify_id")
ingest_dimension("artist", 'spotify_data', artists_dataframe, "artist_spotify_id")


