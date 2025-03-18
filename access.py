import os, signal
import requests
import urllib.parse 
from flask import Flask, redirect, jsonify, request, session, render_template
import base64
from datetime import datetime, timedelta
import pandas as pd
from etl import return_dataframe, transform_df
import json

def create_app():

    app = Flask(__name__)
    app.secret_key = '9elrSMVWeIuOBCBPOLlnIoDF'

    CLIENT_ID = os.getenv("CLIENT_ID")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")
    REDIRECT_URI = 'http://localhost:8888/callback'

    print(CLIENT_ID)
    print(CLIENT_SECRET)

    AUTH_URL = 'https://accounts.spotify.com/authorize'
    TOKEN_URL = 'https://accounts.spotify.com/api/token'
    API_BASE_URL = 'https://api.spotify.com/v1/'

    JSON_FILE_PATH = 'data.json'

    @app.route('/')
    def login():
        scope = 'user-read-recently-played'

        header = {
            'client_id': CLIENT_ID,
            'response_type': 'code',
            'redirect_uri': REDIRECT_URI,
            'scope': scope,
            #'show_dialog': True
        } 

        auth_url = f'{AUTH_URL}?{urllib.parse.urlencode(header)}'

        return redirect(auth_url)

    @app.route('/callback')
    def callback():
        if 'error' in request.args:
            return jsonify({"error": request.args['error']})

        if 'code' in request.args:
            token_data ={
                'code':request.args['code'],
                'grant_type':'authorization_code',
                'redirect_uri': REDIRECT_URI,
            }

            client_creds = f"{CLIENT_ID}:{CLIENT_SECRET}"
            client_creds_b64 = base64.b64encode(client_creds.encode())

            token_header = {
                'Authorization':f'Basic {client_creds_b64.decode()}'
            }
            
            response = requests.post(TOKEN_URL, data=token_data,  headers=token_header)
            token_info = response.json()

            session['access_token'] = token_info['access_token']
            #refresh_token = token_info['refresh_token']
            expires_in = token_info['expires_in'] # seconds
            token_type = token_info['token_type']

            # Need to save the token info somewhere...

            return redirect('/data_pull')
        
    @app.route('/data_pull')
    def data_pull():
        
        header = {
            'Authorization': f'Bearer {session['access_token']}',
            "Accept":"application/json",
            "Content-Type":"application/json",
        }

        today = datetime.now()
        yesterday = today - timedelta(days=1)
        yesterday_unix = int(yesterday.timestamp())*1000

        # Using API to pull after "yesterday" (or the last 24 hours)
        response = requests.get(f"https://api.spotify.com/v1/me/player/recently-played?limit=50&after={yesterday_unix}", headers = header)

        data_pull = response.json()

        if os.path.exists(JSON_FILE_PATH):
            with open(JSON_FILE_PATH, 'w') as file:
                json.dump(data_pull, file, indent=4)
                return redirect('/shutdown')
        else:
            return jsonify({"message": "data.json does not exist, no data written"})


    @app.route('/shutdown')
    def shutdown():
        os.kill(os.getpid(), signal.SIGINT)
        return "Data written to data.json successfully. Server shutting down..."
    
    return app
  
# if __name__ == '__main__':
#     app.run(host='0.0.0.0', debug=True, port=8888)
