import pandas as pd
from datetime import datetime, timedelta
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import json
import sqlite3
import sys
from spotify_etl.extract import get_access_token, get_data

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
    df_transform['id'] = df_transform['timestamp'].astype(str) + "-" + df_transform['artist_name']
    # Strip special characters and white space
    df_transform['id'] = df_transform['id'].str.replace('\\W','', regex=True)

    return df_transform[['id','timestamp','artist_name','count']]

def etl(raw_data):

    # Extract initial data (initial API pull)
    df_songs = return_dataframe(raw_data)

    # Run data quality check
    data_quality(df_songs)
    # Transform data
    df_transform= transform_df(df_songs)

    return df_songs, df_transform

if __name__ == '__main__':
    access_token = get_access_token()
    data_pull = get_data(access_token)
    df_songs, df_transform = etl(data_pull)

    print(df_songs)
    print(df_transform)


