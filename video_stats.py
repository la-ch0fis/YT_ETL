import requests, json
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLER = "Koala.puffss"
maxResults = 50

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
        # print(channel_playlistId)

        return channel_playlistId

    except requests.exceptions.RequestException as e:
        raise e



def get_video_ids(playlist_id):
    ''' According to the API docs, we need a page token because the max number of videos displayed per page is 50, so we need page tokens
     to get all videos of the YT channel '''
    
    video_ids = [] # Contains all videos of the channel
    page_token = None

    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlist_id}&key={API_KEY}"

    try:
        while True:
            url = base_url
            if page_token:
                url += f"&pageToken={page_token}" # I had to add the & symbol to the pageToken variable, because it was failing. The & acts as a concatenation character
                                                  # when the url is being created when iterating through the page tokens
            response = requests.get(url)
            response.raise_for_status()
            data = response.json() 

            for item in data.get('items', []):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)
            # Now we need to specify the next page token in order to get the next batch of videos
            page_token = data.get('nextPageToken')
            # Finally, we exit the loop if there are no more page tokens, meaning there are no more videos
            if not page_token:
                break

        return video_ids

    except requests.exceptions.RequestException as e:
        raise e



if __name__ == "__main__":
    playlist_id = get_playlist_id()
    get_video_ids(playlist_id)
