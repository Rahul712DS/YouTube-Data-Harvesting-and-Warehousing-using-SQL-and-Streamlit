from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd

# 🔐 Setup YouTube client
api_key = "your api key" #it can be acquired through google developer sign in
youtube = build('youtube', 'v3', developerKey=api_key)

def get_channel_ids(query="data science", max_results=10): #query helps search the youtube channel according to the type of channel enter in query
    search_response = youtube.search().list( # in search_response has item, id in JSON/dic format, inside item dic it has snippet,id as key. 
        q=query,
        part='snippet',#in snippet it has publishedAt,channelId,title,description, this the required parameter.
        type='channel',
        maxResults=max_results #max result will limit to 10 no of channels.
    ).execute()

    return [item['id']['channelId'] for item in search_response['items']] #it will return the channel id


def get_channel_data(channel_ids):
    channel_response = youtube.channels().list( # it will item as JSON/dic it have key id,snippet, statistics, content details.
        part='snippet,statistics,contentDetails', #statistics as a JSON/dic will have viewCount, subscriberCount,videoCount etc. 
        # inside content details as dic one more dic as relatedPlaylists which will have likes,Playlists id.
        id=','.join(channel_ids) #single comma-separated string.
    ).execute()

    channel_data = []  # it will content the all channel details.
    playlist_data = [] # it will content the all playlist data of each channel.
    
    for channel_info in channel_response['items']:# each channel_info content the details of snippet,statistics,contentdetails for each channel id.
        channel_data.append({
            "channel_Name": channel_info['snippet']['title'],
            "channel_id": channel_info['id'],
            "Subscription_Count": channel_info['statistics']['subscriberCount'],
            "Channel_Views": channel_info['statistics']['viewCount'],
            "Channel_Description": channel_info['snippet']['description'],
            "Playlist_Id": channel_info['contentDetails']['relatedPlaylists']['uploads']
        })

        playlist_data.append({
            "playlist_id": channel_info['contentDetails']['relatedPlaylists']['uploads'],
            "channel_id": channel_info['id'],
            "playlist_name": f"{channel_info['snippet']['title']} uploads"
        })
    return channel_data, playlist_data


def get_video_ids(uploads_playlist_ids):
    all_video_ids = [] #it will content all the video id for all channel
    video_playlist_map = {} #it will content map video id with respective upload id
    for pid in uploads_playlist_ids:
            video_response = youtube.playlistItems().list( # it will item as JSON/dic in this key as id,contentDetails.
            part='contentDetails',#contentDetails will have videoId
            playlistId=pid,
            maxResults=50 #You can fetch up to 50 items per request using maxResults=50 it is max limit.
            ).execute()

            ids = [item['contentDetails']['videoId'] for item in video_response['items']] #it will retrieve the video ids
            all_video_ids.extend(ids) #it will stack the id one after other in list 
            for item in video_response['items']: 
                vid = item['contentDetails']['videoId']
                video_playlist_map[vid] = pid # it will create the key pair with each video id to there respective upload id
            
    return all_video_ids,video_playlist_map


def get_video_details(video_ids,video_playlist_map):
    video_data = [] # it will content all video data for each video id

    for i in range(0, len(video_ids), 50):  # chunked to avoid API limits, which is 50 
        chunk = video_ids[i:i+50]
        video_response = youtube.videos().list( # it will item as json/dic in which key will snippet, statistics,content details.
            part='snippet,statistics,contentDetails',
            id=','.join(chunk)
        ).execute()

        for video in video_response['items']:
            snippet = video['snippet']
            stats = video['statistics']
            content = video['contentDetails']
            video_data.append({
                "Video_Id": video['id'],
                "playlist_id": video_playlist_map.get( video['id'], None), #get will help to assign palylist id to each video id
                "Video_Name": snippet['title'],
                "Video_Description": snippet['description'],
                "Tags": snippet.get('tags', []),
                "PublishedAt": snippet['publishedAt'],
                "View_Count": stats.get('viewCount', '0'), # get will help us get viewCount if there is none than it will be zero.
                "Like_Count": stats.get('likeCount', '0'),
                "Favorite_Count": stats.get('favoriteCount', '0'),
                "Comment_Count": stats.get('commentCount', '0'),
                "Duration": content['duration'],
                "Thumbnail": snippet['thumbnails']['high']['url'],
                "Caption_Status": content['caption']
            })

    return video_data


def get_comment_details(video_ids):
    comment_data = [] #it will content all the comment data for each video

    for id_s in video_ids:
        try:
            comments_response = youtube.commentThreads().list( # it fetch topLevelComment, each id
                part='snippet',
                videoId=id_s,
                maxResults=100
                ).execute()

            for item in comments_response.get('items', []): #here get will help to handle if video doesn't have comment than it handle it and return empty list
                comment = item['snippet']['topLevelComment']['snippet']
                comment_data.append({
                    "Comment_Id": item['id'],
                    "video_id": item['snippet']['videoId'],
                    "Comment_Text": comment['textDisplay'],
                    "Comment_Author": comment['authorDisplayName'],
                    "Comment_PublishedAt": comment['publishedAt']
                })

                        
        except Exception as e:
            print(f"Skipping video {id_s} due to error: {e}")

    return comment_data

def convert_to_dataframes(channel_data, playlist_data,video_data, comment_data):
    """Converts raw data lists into pandas DataFrames."""
    channel_df = pd.DataFrame(channel_data)
    playlist_df = pd.DataFrame(playlist_data)
    video_df = pd.DataFrame(video_data)
    comment_df = pd.DataFrame(comment_data)
    return channel_df, playlist_df, video_df, comment_df



