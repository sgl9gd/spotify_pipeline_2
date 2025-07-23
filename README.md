# Introduction

This project was inspired by sidarth1805: [link](https://github.com/sidharth1805/Spotify_etl.git). I started by trying to re-create the pipeline one-for-one but found it impossible since much of the API documentation has been updated. It's also called spotify_pipline_2 because spotify_pipeline_1...has my CLIENT SECRET all over it. Well, either way, below I'll walk through a bit of my thought process and how I ended up with the final product.

# Initial Attempt - Flask

I wanted to create a pipeline that would record what I had listened to in the past 24 hours. In the original project, getting the API token and access to user played tracks was more straightforward. Developing the pipeline in 2025 required going through an OAuth 2.0 process to get user authorization and an access code.

I searched the internet and found the official Spotipy package that could help with this, but I wanted to see if I could do it the traditional way using just requests. You'll see in my \_drafts folder that I started off by making a Flask app that went through the OAuth 2.0 steps to get an access token. From there, I followed the steps from sidarth1805 and was able to create a simple pipeline that uploaded to a SQLite table. Then came the challenge of running things in Airflow.

# Airflow and Docker

Before I even tried running anything in Airflow... I had to get it up and running. This was my first time using Docker and Airflow, and I'll admit it was a bit of a struggle. Getting the Docker UI running required reconfiguring my PC (can’t really remember the exact steps, but I followed some documentation/watched some YouTube videos). From there... Airflow took a long time to spin up. I used the YAML file from their website, but things just wouldn’t run. A few things I know I had to figure out to get it working:

- Generate (or find?) the JWT secret (this took some time to figure out — to be honest, I’ll have to re-learn this next time I spin up a new YAML file)
- Configure an AIRFLOW_UID value in a .env
- Properly connect and pip install Postgres-related packages

These are just a few things I remember off the top of my head, but I spent a full night just debugging the setup. It felt like a marathon, but I’m sure next time it’ll go much faster. I will say, working with the most recent version (as of this pipeline — 3.0.3) didn’t help, since there were some bugs with the JWT secret.

# Re-working the pipeline

Once I got Airflow to actually run... I realized a Flask app wasn’t the most sustainable solution in that environment. After doing some research, I found that using a refresh token was the best way to go. I reconfigured my auth logic into the get_token file so I could retrieve a refresh token. Once that was obtained, I reworked my ETL functions to use the refresh token to get a new access token, pull data, and transform it into my final desired output.

From there, I created my final DAG that established the tables and ran the full pipeline. Definitely a lot of trial and error here as well, but I'm really happy I was able to create a small end-to-end pipeline.

# Final Thoughts

ChatGPT... is something for sure. This project already took longer than I'd have liked due to work obligations, but I feel like it might’ve taken twice as long without ChatGPT. It helped me build the DAG and YAML file framework, and was a godsend for debugging. It’s honestly really cool. BUT, I definitely feel the drawbacks of it. I can see myself eventually becoming over-reliant and possibly learning less than I’d like. I had to stop a few times and force myself to read the Airflow/Docker documentation so I was actually learning something. I tried to avoid copy/pasting directly from ChatGPT at times so I could at least go through the motions of coding things myself.

I think the biggest drawback, if overused, is that it can really affect retention. Being able to ask it anything and get an immediate answer is incredibly convenient, but not conducive to actual learning. That being said, it's still such a great learning tool. If used correctly, I can guide myself in the right direction, and it feels like I can effectively teach myself anything. It's definitely a double-edged sword.

That being said... this README was 100% edited by ChatGPT :)

# Running this code

If anyone is really interested in re-running this, I'll give some brief steps:

1. Spin up a venv using the requirements.txt
2. Go to the Spotify Developoers Dashboard:[link](https://developer.spotify.com/dashboard) to create an app to get your Client ID/Secret
3. Run get_tokens.py to get your Refresh Token
4. Spin up Docker and then use the docker-compose.yaml to spin up Airflow (not sure if this repo is has everything need to get all this configured, but the foundation is here. Recommend looking through documentaiton/tutorials if this is your first time using Docker/Airflow)
5. Run the DAG!

This is definitely an oversimplified check list - I am sure there are things I missed. Good luck and let me know if you have any questions. I had a ton of fun making this pipeline - on to the next one!
