import requests, json
# import os
from datetime import date 
# from dotenv import load_dotenv
from airflow.decorators import task
from airflow.models import Variable

# load_dotenv(dotenv_path="./.env")

# API_KEY = os.getenv("API_KEY")
# CHANNEL_HANDLE = os.getenv("CHANNEL_HANDLE")
API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")


maxResults = 50

@task
def get_playlist_id():
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

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

        #now we get the playlist id:
        channel_playlistId = channel_items['contentDetails']['relatedPlaylists']['uploads']
        # print(channel_playlistId)

        return channel_playlistId

    except requests.exceptions.RequestException as e:
        raise e


@task
def get_video_ids(playlist_id):
    """ According to the API docs, we need a page token because the max number of videos displayed per page is 50, so we need page tokens
     to get all videos of the YT channel """
    
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


""" def batch_list(video_id_list, batch_size):
    Ok, Cochise, listen up, this "yield" keyword is a game-changer. 
     The breakdown: "return" vs "yield" 
     In a normal function, return is the end of the line. It gives you the data, kills the function, and clears the memory.
     "yield" is different, cuz, it creates a GENERATOR. Instead of finishing, the function "pauses", gives you one piece of data, 
     and waits for you to ask for the next one. 
     Walking through batch_list
     This specific function is a "Batcher." It's designed to take a massive list (like 1 million video_ids) and break it into small, manageable chunks 
     so you don't crash your RAM or hit an API rate limit.
     range(0, len(video_id_list), batch_size): This creates a loop that jumps by the batch_size. If the size is 10, it goes 0, 10, 20, 30...

     video_id_list[video_id: video_id + batch_size]: This slices the list. If video_id is 0 and batch_size is 10, it grabs items 0 through 9.

     The yield part:
     Instead of making a new big list of lists, it produces one slice at a time.

     The function "remembers" where it left off.
     When the next loop starts, it picks up exactly at the next video_id.
     

    for video_id in range(0, len(video_id_list), batch_size):
        yield video_id_list[video_id: video_id + batch_size] """

@task
def extract_video_data(video_ids):
    extracted_data = []

    def batch_list(video_id_list, batch_size):
        for video_id in range(0, len(video_id_list), batch_size):
            yield video_id_list[video_id: video_id + batch_size] 
    # We loop through the whole list in batches with the different video ids using the join method for the URL build
    # So, it'll be something like this: 'EDRRT45b', 'rwn-XXymNG4', 'Snd-VYLx064'  
    try:
        for batch in batch_list(video_ids, maxResults):
            video_ids_str = ",".join(batch)
            base_url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"
            # Then we check for errors and get the response in JSON format
            response = requests.get(base_url)
            response.raise_for_status()
            data = response.json()
            # Now we use the get method to get the values we need and build a dictionary: videoId, snippet, content details and statistics
            # We'll use the empty list defined at the beginning as a value to return if the specified key does not exist.
            for item in data.get("items", []):
                video_id = item["id"]
                snippet = item["snippet"]
                contentDetails = item["contentDetails"] 
                statistics = item["statistics"]
                # We populate the dictionary with the obtained values
                video_data = {
                    "video_id": video_id,
                    "title": snippet["title"],
                    "publishedAt": snippet["publishedAt"],
                    "duration": contentDetails["duration"],
                    "viewCount": statistics.get("viewCount", None),
                    "likeCount": statistics.get("likeCount", None),
                    "commentCount": statistics.get("commentCount", None)
                }
                # The we append the data and return it
                extracted_data.append(video_data)
                
        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e

@task
def save_to_json(extracted_data):
    """ This function will save the data related to the videos we extratced and save it in a JSON file """
    file_path = f"./data/YT_data_{date.today()}.json"
    # utf-8 encoding ensures that the file can handle special characters
    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)
