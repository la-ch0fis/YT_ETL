import requests, json
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLER = "Koala.puffss"

def get_playlist_id():
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLER}&key={API_KEY}"

        response = requests.get(url)
        response.raise_for_status()

        # showcase data and printing ouput
        #  json.dumps: it turns Python objects into JSON strings so you can send that shit over the wire, save it to files, or feed it to APIs.
        # NOTE: ident param makes it human readable, and 4 is the standard and a good practice
        data = response.json()
        # print(json.dumps(data,indent=4))
        # Getting the playlist id, not to be confused with the channel id which has the 'id' key
        # we can navigate a JSON file using the --> [] 
        data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
        # Now that we see the stracutre, we get the idea of how to navigate the python data dictionary which is a JSON of course
        channel_items = data['items'][0]
        # print(channel_items)

        #now we get the playlost id:
        channel_playlistId = channel_items['contentDetails']['relatedPlaylists']['uploads']
        print(channel_playlistId)

        return channel_playlistId

    except requests.exceptions.RequestException as e:
        raise e
    

if __name__ == "__main__":
    get_playlist_id()
