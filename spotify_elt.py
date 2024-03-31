import spotipy
import pandas as pd
import json 
from datetime import datetime
import s3fs
import csv
import base64
from spotipy import SpotifyClientCredentials
from requests import post ,get

client_id = "897d200d433d4baabd0c8af43d2da7ea"
client_secret_id = "69da8e53f9f34bac87b07547b170e1e1"

def get_token():
    auth_string = client_id + ":" + client_secret_id
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes) , "utf-8")

    url = 'https://accounts.spotify.com/api/token'

    headers = {
        "Authorization" : "Basic " + auth_base64,
        "Content-Type" : "application/x-www-form-urlencoded"
    }

    data = {"grant_type":"client_credentials"}
    result = post(url,headers=headers,data=data)
    json_result = json.loads(result.content)
    token = json_result['access_token']
    return token

def get_auth_header(token):
    return {"Authorization" : "Bearer " + token}

def search_for_artist(token , artist_name):
    url = "https://api.spotify.com/v1/search"
    headers = get_auth_header(token)
    query = f"?q={artist_name}&type=artist"

    query_url = url + query
    result = get(query_url,headers = headers)
    json_result = json.loads(result.content)["artists"]["items"]
    if len(json_result) == 0:
        print("no artist found")
        return None
    else:
        return json_result
    
    
def get_songs_by_artists(token,artist_id):
    url = f"http://api.spotify.com/v1/artists/{artist_id}/top-tracks?country=IN"
    headers = get_auth_header(token)
    result = get(url,headers = headers)
    json_result = json.loads(result.content)["tracks"]
    return json_result



def run_spotify_elt():
    client_id = "897d200d433d4baabd0c8af43d2da7ea"
    client_secret_id = "69da8e53f9f34bac87b07547b170e1e1"


    token = get_token()
    result = search_for_artist(token ,"Amit Trivedi")
    print(type(result))
    print(len(result))
    print(type(result[0]))
    # data = result["artists"]['items']

    data_list = []
    for i in result:

        data = {
            "artist_name" : i["name"],
            "artist_id" : i["id"],
            "artist_followers" : i["followers"]["total"],
            "artist_generes" : i['genres']
        }

        data_list.append(data)

    df = pd.DataFrame(data_list)
    df.to_csv("s3://airflow-spotify-bucket-bhavya/Amit_Trivedi_spotify_data")

run_spotify_elt()

# artist_id = result['id']
# songs = get_songs_by_artists(token , artist_id)

# for id , song in enumerate(songs):
#     print(f"{id+1}. {song['name']}")



    

    


