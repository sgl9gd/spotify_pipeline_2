import pandas as pd
import requests
from dotenv import load_dotenv
import os
import base64
from datetime import datetime, timedelta

#Get your main directory (top layer)
main_dir = os.getcwd()
load_dotenv(dotenv_path=os.path.join(main_dir,".env"))

# Get refresh token, client ID/SECRET info
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
TOKEN_URL = 'https://accounts.spotify.com/api/token'

#Use refresh token to conduct Spotify API call

def get_access_token(client_id, client_secret, refresh_token):
    scope = 'user-read-recently-played'

    client_creds = F"{client_id}:{client_secret}"
    client_creds_b64 = base64.b64encode(client_creds.encode()).decode()

    headers = {
        "Authorization": f"Basic {client_creds_b64}",
        "Content-Type": "application/x-www-form-urlencoded"
    } 

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }

    response = requests.post(TOKEN_URL, headers=headers, data=data)
    # response.raise_for_status()
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        print("Error response:", response.text)
        raise

    return response.json()["access_token"]

def get_data(access_token):

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type":"application/json"
    }

    today = datetime.now()
    yesterday = today - timedelta(days=1)
    yesterday_unix = int(yesterday.timestamp())*1000

    # Using PAI to pull data from all of "yesterday" (or last 24 hours)
    response = requests.get(f"https://api.spotify.com/v1/me/player/recently-played?limit=50&after={yesterday_unix}", headers = headers)

    return response.json()

if __name__ == '__main__':
    access_token = get_access_token(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
    print(access_token)
    test = get_data(access_token)
