import streamlit as st
from sql_migration import insert_data_to_mysql,get_channel_summary
import pandas as pd
import altair as alt
import plotly.express as px

from api_functions import (
    get_channel_ids,
    get_channel_data,
    get_video_ids,
    get_video_details,
    get_comment_details,
    convert_to_dataframes
)

st.title("📺 YouTube Data Harvesting and Warehousing using SQL and Streamlit")

# 1. User Inputs
query = st.text_input("Enter channel topic (e.g. 'data science')", value="data science")
max_channels = st.slider("Number of channels to fetch", 1,10)

# Step 1: Fetch channel IDs (runs only when button is pressed)
if st.button("🔍 Fetch Channel Data"):
    with st.spinner("Fetching channel IDs..."):
        st.session_state.fetched_channel_ids = get_channel_ids(query, max_channels) 
#st.session_state, it remembers the value between each button click (i.e., between reruns of the script).
# Step 2: Show multiselect + manual entry once IDs are available
if "fetched_channel_ids" in st.session_state:
    st.subheader("🎯 Choose Channel IDs to Analyze")

    selected_channels = st.multiselect(
        "Select from fetched channels",
        st.session_state.fetched_channel_ids,
        label_visibility="visible"
    )

    manual_channel_id = st.text_input("Or enter a channel ID manually (optional)")

    # Merge both
    combined_channel_ids = selected_channels.copy()
    if manual_channel_id:
        combined_channel_ids.append(manual_channel_id)

    if combined_channel_ids:
        with st.spinner("Retrieving channel details..."):
            channel_data, playlist_data = get_channel_data(combined_channel_ids)
        st.success("Channels Data Retrieved!")
        # Get uploads playlist IDs
        with st.spinner("Retrieving youtube upload Ids..."):
            playlist_ids = [c['Playlist_Id'] for c in channel_data]
        st.success("Uploads Ids Retrieved")     

        with st.spinner("Getting video IDs..."):
            video_ids,video_playlist_map = get_video_ids(playlist_ids)
        st.success("Videos Ids Retrieved")    

        with st.spinner("Fetching video metadata..."):
            videos = get_video_details(video_ids,video_playlist_map)
        st.success(f"Fetched metadata for {len(videos)} videos.")
        all_comments = []
        for video in videos:
            with st.spinner("Fetching top comments..."):
                comment = get_comment_details([video['Video_Id']])
                all_comments.extend(comment)
                #if comment:
                    #for c in comment:
                        #pass
                #else:
                    #st.write("No comments available for this video.")
        st.success(f"Fetched {len(comment)} comments.")         

        channel_df, playlist_df, video_df, comment_df = convert_to_dataframes(channel_data,playlist_data,videos,all_comments)
        st.subheader("📊 Channel Data")
        st.dataframe(channel_df)

        st.subheader(" Playlist Data")
        st.dataframe(playlist_df)
        
        st.subheader("📽 Video Data")
        st.dataframe(video_df)

        st.subheader("💬 Comment Data")
        st.dataframe(comment_df)

        
        st.subheader("🙳️ Insert Data into SQL Server")
        user_confirmation = st.radio("Do you want to insert the data into the SQL database?", ["No", "Yes"])
        if user_confirmation == "Yes":
            if st.button("✅ Confirm Insert"):
                insert_data_to_mysql(channel_df, playlist_df, video_df, comment_df)
                st.success("✅ Data inserted in SQL Database successfully.")
            else:
                st.info("Data not inserted. Select 'Yes' and confirm to proceed.")


        with st.spinner("Querying SQL database..."):
            query_options = list([
                    "1. What are the names of all the videos and their corresponding channels?",
                    "2. Which channels have the most number of videos, and how many videos do they have?",
                    "3. What are the top 10 most viewed videos and their respective channels?",
                    "4. How many comments were made on each video, and what are their corresponding video names?",
                    "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                    "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                    "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                    "8. What are the names of all the channels that have published videos in the year 2022?",
                    "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                    "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
                    ])
            selection = st.multiselect("📌 Select one or more queries to run", query_options)
            if st.button("📦 Run Selected Queries"):
                with st.spinner("Running SQL queries..."):
                    result_map = get_channel_summary(selection)
                    if result_map:
                        # For each query result, gives the query text as question, and unpack the result tuple into rows and columns
                        for question,(rows, columns) in result_map.items(): 
                            st.subheader(question)
                            if not rows:
                                st.warning(f"No data returned for: {question}")
                                continue

                            df = pd.DataFrame(rows, columns=columns)
                            st.dataframe(df)

                            # Visualization logic per question
                            if "2. Which channels have the most number of videos" in question:
                                chart = alt.Chart(df).mark_bar().encode(
                                    x=alt.X("channel_name:N", sort='-y'), #:N means it’s a nominal (categorical/text) variable,sort='-y' means sort bars    descending by Y value (video count).
                                    y="video_count:Q", #:Q means it’s a quantitative (numeric) value.
                                    tooltip=["channel_name", "video_count"] #When you hover over a bar, a tooltip shows the channel name and video count.
                                ).properties(title="Channels with the Most Videos") #Adds a title above the chart: "Channels with the Most Videos"
                                st.altair_chart(chart, use_container_width=True) #Renders the Altair chart in your Streamlit app.

                            elif "3. What are the top 10 most viewed videos" in question:
                                chart = alt.Chart(df).mark_bar().encode(
                                    x=alt.X("video_name:N", sort='-y'),
                                    y="view_count:Q",
                                    tooltip=["video_name", "view_count", "channel_name"]
                                    ).properties(title="Top 10 Most Viewed Videos")
                                st.altair_chart(chart, use_container_width=True)

                            elif "7. What is the total number of views for each channel" in question:
                                chart = alt.Chart(df).mark_bar().encode(
                                        x=alt.X("channel_name:N", sort='-y'),
                                        y="total_views:Q",
                                        tooltip=["channel_name", "total_views"]
                                ).properties(title="Total Views per Channel")
                                st.altair_chart(chart, use_container_width=True)
                            
                            elif "9. What is the average duration of all videos in each channel" in question:
                                chart = alt.Chart(df).mark_bar().encode(
                                    x=alt.X("channel_name:N", sort='-y'),
                                    y="avg_duration:Q",
                                    tooltip=["channel_name", "avg_duration"]
                                ).properties(title="Average Video Duration per Channel (seconds)")
                                st.altair_chart(chart, use_container_width=True)
                            
                            elif "10. Which videos have the highest number of comments" in question:
                                chart = alt.Chart(df).mark_bar().encode(
                                    x=alt.X("video_name:N", sort='-y'),
                                    y="comment_count:Q",
                                    tooltip=["video_name", "comment_count", "channel_name"]
                                ).properties(title="Most Commented Videos")
                                st.altair_chart(chart, use_container_width=True)
                    else:
                        st.warning("No results found for selected queries.")

                
               