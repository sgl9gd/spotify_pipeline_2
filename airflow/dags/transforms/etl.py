import pandas as pd
from datetime import datetime, timedelta
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import json
import sqlite3
import sys

def return_dataframe(data):
    '''
    This function will use the access token to pull recently played data in past 24 hours from user play history.
    '''

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Extracting relevant data
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    # Create a dictionary that will be converted into a DataFrame
    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    df_songs = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])
    
    return df_songs

def data_quality(df):
    '''
    This function will do basic checks to ensure data quality is sufficient. 
    '''

    if df.empty:
        print('No songs extracted')
        return False
    
    # Now we will enforce a primary key since we do not need duplicates
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        # using Exception so that the program is immediately terminated
        raise Exception("Primary Key Exception: Data may contain duplicates")
    
    if df.isnull().values.any():
        raise Exception("Null Values Found")

def transform_df(df):
    '''
    This function will apply transformation logic.
    '''
    
    df_transform = df.groupby(['timestamp','artist_name'],as_index=False).count()
    df_transform = df_transform.rename(columns={'played_at':'count'})

    # Creating a Primary Key based on TimeStamp and Arist
    df_transform['ID'] = df_transform['timestamp'].astype(str) + "-" + df_transform['artist_name']
    # Strip special characters and white space
    df_transform['ID'] = df_transform['ID'].str.replace('\\W','', regex=True)

    return df_transform[['ID','timestamp','artist_name','count']]

def spotify_etl():

    try:
        with open('data.json', 'r') as file:
            data = json.load(file)
            if len(data) == 0:
                print('no data in json file')
                sys.exit()
    except FileNotFoundError:
        print("Error: JSON file not found.")
    except json.JSONDecodeError:
        print("Error: Invalid JSON format in the file.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    # Extract initial data (initial API pull)
    df_songs = return_dataframe(data)

    # Run data quality check
    data_quality(df_songs)
    # Transform data
    df_transform= transform_df(df_songs)

    return df_songs, df_transform

# def load_df(df_songs, df_transform, database_filepath):

#     # Loading into Database
#     engine = sqlalchemy.create_engine(database_filepath)
#     conn = sqlite3.connect('my_played_tracks.sqlite')
#     cursor = conn.cursor()

#     sql_query_1 = '''
#     CREATE TABLE IF NOT EXISTS tracks_streamed(
#         song_name VARCHAR(200),
#         artist_name VARCHAR(200),
#         played_at VARCHAR(200),
#         timestamp VARCHAR(200),
#         CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
#     )
#     '''

#     sql_query_2 ='''
#     CREATE TABLE IF NOT EXISTS artist_track_count(
#         timestamp VARCHAR(200),
#         ID VARCHAR(200),
#         artist_name VARCHAR(200),
#         count VARCHAR(200),
#         CONSTRAINT primary_key_constraint PRIMARY KEY (ID)
#         )
#     '''
#     cursor.execute(sql_query_1)
#     cursor.execute(sql_query_2)
#     print("Opened database successfully")

#     # Use a series of try/excepts to load data into the database
#     try:
#         df_songs.to_sql('tracks_streamed', engine, index=False, if_exists='append')
#         print('tracks_streamed updated')
#     except:
#         print('This data already exists in my_played_tracks')
#     try:
#         df_transform.to_sql('artist_track_count', engine, index=False, if_exists='append')
#         print('artist_track_count updated')
#     except:
#         print('This data already exists in artist_track_count')

#     # Close the database
#     conn.close()
#     print('Closed database successfully')


# def load_df(df_songs, df_transform, database_filepath):
#     engine = sqlalchemy.create_engine(database_filepath)

#     with engine.connect() as connection:
#         connection.execute('''
#             CREATE TABLE IF NOT EXISTS tracks_streamed(
#                 song_name VARCHAR(200),
#                 artist_name VARCHAR(200),
#                 played_at VARCHAR(200),
#                 timestamp VARCHAR(200),
#                 CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
#             )
#         ''')

#         connection.execute('''
#             CREATE TABLE IF NOT EXISTS artist_track_count(
#                 timestamp VARCHAR(200),
#                 ID VARCHAR(200),
#                 artist_name VARCHAR(200),
#                 count VARCHAR(200),
#                 CONSTRAINT primary_key_constraint PRIMARY KEY (ID)
#             )
#         ''')

#         # test = connection.execute(
#         # '''
#         # SELECT * FROM tracks_streamed
#         # LIMIT 10
#         # ''')
#         # for row in test:
#         #     print(row)

#     print("Opened database successfully")

#     try:
#         df_songs.to_sql('tracks_streamed', engine, index=False, if_exists='append')
#         print('tracks_streamed updated')
#     except:
#         print('This data already exists in my_played_tracks')

#     try:
#         df_transform.to_sql('artist_track_count', engine, index=False, if_exists='append')
#         print('artist_track_count updated')
#     except:
#         print('This data already exists in artist_track_count')

#     print('Closed database successfully')



# spotify_etl()

# if __name__ == '__main__':

#     write_to_db = True

#     try:
#         with open('data.json', 'r') as file:
#             data = json.load(file)
#             if len(data) == 0:
#                 print('no data in json file')
#                 sys.exit()
#     except FileNotFoundError:
#         print("Error: JSON file not found.")
#     except json.JSONDecodeError:
#         print("Error: Invalid JSON format in the file.")
#     except Exception as e:
#         print(f"An unexpected error occurred: {e}")

#     # Extract initial data (initial API pull)
#     df_songs = return_dataframe(data)

#     # Run data quality check
#     data_quality(df_songs)
#     # Transform data
#     df_transform= transform_df(df_songs)

#     # Print raw output
#     print(df_songs)
#     # Print arist count dataframe
#     print(df_transform)

#     if write_to_db:
#         # Set database location
#         database_filepath = "sqlite:///my_played_tracks.sqlite"
#         load_df(df_songs, df_transform, database_filepath)
#     else:
#         print('No data uploaded')