import mysql.connector
import isodate
from datetime import datetime

def parse_duration(duration_str):
    try:
        duration = isodate.parse_duration(duration_str)
        return int(duration.total_seconds())
    except Exception:
        return 0

def parse_mysql_datetime(dt_str):
    try:
        return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None

def insert_data_to_mysql(channel_df, playlist_df, video_df, comment_df):
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='root',
        database='youtubedataharvesting'
    )
    cursor = conn.cursor()

    # Insert into channel
    #_ is a placeholder meaning “ignore the index.”,row is a Series representing one row of data (with column names as keys).
    #DUPLICATE KEY UPDATE If a row with the same channel_id already exists (primary or unique key conflict), it updates the existing row instead of inserting a      new one.
    for _, row in channel_df.iterrows(): 
        cursor.execute("""
            INSERT INTO channel (channel_id, channel_name, channel_type, channel_views, channel_description, channel_status)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                channel_name = VALUES(channel_name),
                channel_type = VALUES(channel_type),
                channel_views = VALUES(channel_views),
                channel_description = VALUES(channel_description),
                channel_status = VALUES(channel_status)
        """, (
            row['channel_id'],
            row['channel_Name'],
            row.get('channel_type', 'N/A'),
            int(row.get('Subscription_Count', 0)),
            row.get('Channel_Description', ''),
            row.get('channel_status', 'active')
        ))

    # Insert into playlist
    for _, row in playlist_df.iterrows():
        cursor.execute("""
            INSERT INTO playlist (playlist_id, channel_id, playlist_name)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                playlist_name = VALUES(playlist_name)
        """, (
            row['playlist_id'],
            row['channel_id'],
            row['playlist_name']
        ))

    # Insert into video
    for _, row in video_df.iterrows():
        cursor.execute("""
            INSERT INTO video (
                video_id, playlist_id, video_name, video_description, published_date,
                view_count, like_count, dislike_count, favorite_count, comment_count,
                duration, thumbnail, caption_status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                video_name = VALUES(video_name),
                video_description = VALUES(video_description),
                view_count = VALUES(view_count),
                like_count = VALUES(like_count),
                favorite_count = VALUES(favorite_count),
                comment_count = VALUES(comment_count),
                duration = VALUES(duration),
                thumbnail = VALUES(thumbnail),
                caption_status = VALUES(caption_status)
        """, (
            row['Video_Id'],
            row.get('playlist_id', None),
            row['Video_Name'],
            row['Video_Description'],
            parse_mysql_datetime(row['PublishedAt']),
            int(row.get('View_Count', 0)),
            int(row.get('Like_Count', 0)),
            int(row.get('Dislike_Count', 0)) if 'Dislike_Count' in row else 0,
            int(row.get('Favorite_Count', 0)),
            int(row.get('Comment_Count', 0)),
            parse_duration(row['Duration']),
            row['Thumbnail'],
            row['Caption_Status']
        ))

    # Insert into comment
    for _, row in comment_df.iterrows():
        cursor.execute("""
            INSERT INTO comment (
                comment_id, video_id, comment_text, comment_author, comment_published_date
            )
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                comment_text = VALUES(comment_text),
                comment_author = VALUES(comment_author),
                comment_published_date = VALUES(comment_published_date)
        """, (
            row['Comment_Id'],
            row.get('video_id', None),
            row['Comment_Text'],
            row['Comment_Author'],
            parse_mysql_datetime(row['Comment_PublishedAt'])
        ))

    conn.commit()
    cursor.close()
    conn.close()

def get_channel_summary(selection):
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='root',
        database='youtubedataharvesting'
    )
    cursor = conn.cursor()

    # Add all queries here
    query_map = {
        "1. What are the names of all the videos and their corresponding channels?": """
            SELECT c.channel_name, v.video_name
            FROM channel c
            LEFT JOIN playlist p ON c.channel_id = p.channel_id
            LEFT JOIN video v ON p.playlist_id = v.playlist_id;
        """,
        "2. Which channels have the most number of videos, and how many videos do they have?": """
            SELECT c.channel_name, COUNT(v.video_id) AS video_count
            FROM channel c
            LEFT JOIN playlist p ON c.channel_id = p.channel_id
            LEFT JOIN video v ON p.playlist_id = v.playlist_id
            GROUP BY c.channel_id
            ORDER BY video_count DESC
            LIMIT 1;
        """,
        "3. What are the top 10 most viewed videos and their respective channels?": """
            SELECT c.channel_name, v.video_name, v.view_count
            FROM channel c
            JOIN playlist p ON c.channel_id = p.channel_id
            JOIN video v ON p.playlist_id = v.playlist_id
            ORDER BY v.view_count DESC
            LIMIT 10;
        """,
        "4. How many comments were made on each video, and what are their corresponding video names?": """
            SELECT v.video_name, COUNT(cm.comment_id) AS comment_count
            FROM video v
            LEFT JOIN comment cm ON v.video_id = cm.video_id
            GROUP BY v.video_id;
        """,
        "5. Which videos have the highest number of likes, and what are their corresponding channel names?": """
            SELECT c.channel_name, v.video_name, v.like_count
            FROM channel c
            JOIN playlist p ON c.channel_id = p.channel_id
            JOIN video v ON p.playlist_id = v.playlist_id
            ORDER BY v.like_count DESC
            LIMIT 1;
        """,
        "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?": """
            SELECT video_name, like_count, dislike_count
            FROM video;
        """,
        "7. What is the total number of views for each channel, and what are their corresponding channel names?": """
            SELECT c.channel_name, SUM(v.view_count) AS total_views
            FROM channel c
            JOIN playlist p ON c.channel_id = p.channel_id
            JOIN video v ON p.playlist_id = v.playlist_id
            GROUP BY c.channel_id;
        """,
        "8. What are the names of all the channels that have published videos in the year 2022?": """
            SELECT DISTINCT c.channel_name
            FROM channel c
            JOIN playlist p ON c.channel_id = p.channel_id
            JOIN video v ON p.playlist_id = v.playlist_id
            WHERE YEAR(v.published_date) = 2022;
        """,
        "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?": """
            SELECT c.channel_name, AVG(v.duration) AS avg_duration
            FROM channel c
            JOIN playlist p ON c.channel_id = p.channel_id
            JOIN video v ON p.playlist_id = v.playlist_id
            GROUP BY c.channel_id;
        """,
        "10. Which videos have the highest number of comments, and what are their corresponding channel names?": """
            SELECT c.channel_name, v.video_name, COUNT(cm.comment_id) AS comment_count
            FROM channel c
            JOIN playlist p ON c.channel_id = p.channel_id
            JOIN video v ON p.playlist_id = v.playlist_id
            LEFT JOIN comment cm ON v.video_id = cm.video_id
            GROUP BY v.video_id
            ORDER BY comment_count DESC
            LIMIT 1;
        """
    }

    results = {}
    for sel in selection:
        cursor.execute(query_map[sel])
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] #cursor.description holds metadata about the columns. it gives the column name
        results[sel] = (rows, columns)

    cursor.close()
    conn.close()
    return results


