import os, signal
import requests
import urllib.parse 
from flask import Flask, redirect, jsonify, request, session, render_template
import base64
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv, set_key
# from transforms.etl import return_dataframe, transform_df
import json
import webbrowser

def create_app():

    app = Flask(__name__)
    app.secret_key = '9elrSMVWeIuOBCBPOLlnIoDF'

    load_dotenv()


    CLIENT_ID = os.getenv("CLIENT_ID")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")
    REDIRECT_URI = 'http://127.0.0.1:8888/callback'

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
            'show_dialog': True
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
                'Authorization':f'Basic {client_creds_b64.decode()}',
            }
            
            response = requests.post(TOKEN_URL, data=token_data,  headers=token_header)
            token_info = response.json()


            # Store token for access later
            session['access_token'] = token_info['access_token']
            expires_in = token_info['expires_in'] # seconds
            token_type = token_info['token_type']

            try:
                refresh_token = token_info['refresh_token']
                set_key('.env', 'REFRESH_TOKEN', refresh_token)
                print("Refresh token saved:", refresh_token)
            except KeyError:
                print("Refresh token not found in token_info.")
            except Exception as e:
                print("Something went wrong:", str(e))

            # # Get refresh token and write to .env file
            # refresh_token = token_info['refresh_token']
            # #set for refresh token write path
            # env_path = '.env'
            # set_key(env_path, 'REFRESH_TOKEN', refresh_token)
            
            # Need to save the token info somewhere...

            return redirect('/shutdown')
        
    @app.route('/shutdown')
    def shutdown():
        os.kill(os.getpid(), signal.SIGINT)
        return "Refresh token generated. Server shutting down..."
    
    return app
  
if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=8888)
