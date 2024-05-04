
import googleapiclient.discovery
import googleapiclient.errors
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
from streamlit_option_menu import option_menu
from pathlib import Path





api = "AIzaSyAvu8F4fqFRgvxPPDEDupHyc-aaOGEFkJ4"
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(
api_service_name, api_version, developerKey=api)

id1= st.text_input("enter the channel id",label_visibility="visible")


def channelinfo(id1):
    channeldata=[]
    try:
        request = youtube.channels().list(
            part ="snippet,contentDetails,statistics",
            id =id1

        )
        response = request.execute()
    
        for i in response ['items']:
            data = dict (channelname = i ['snippet']['title'],
                        
                    channelid = i ['id'],
                    subscriber = i ['statistics']['subscriberCount'],
                    views =i ['statistics']['viewCount'],
                    totalvideo = i ['statistics']['videoCount'],
                    channeldescription = i ['snippet']['description'],
                    playlistid = i ['contentDetails']['relatedPlaylists']['uploads'])

            channeldata.append(data) 
    except: 
         pass   
    return  channeldata

get_channel_info = channelinfo(id1)


def videoids (id1):
        vidids =[]
        try:
            vidinfo = youtube.channels().list(
                    part ="contentDetails",
                    id =id1).execute()
            playlistid = vidinfo['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            pagetoken=None
        except:
             pass
        
        try:
                while True:
                        vidinfo1=youtube.playlistItems().list(
                                part='snippet',
                                playlistId=playlistid,
                                pageToken=pagetoken,
                                maxResults=50).execute()
                        for i in range(len(vidinfo1['items'])):
                                vidids.append(vidinfo1['items'][i]['snippet']['resourceId']['videoId'])
                        pagetoken=vidinfo1.get('nextPageToken')

                        if pagetoken is None:
                            break
        except:
             pass            
        return vidids 


vd=videoids(id1)

def viddata(vd):
    videodata=[]
    for vi in vd:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=vi
        )
        response2=request.execute()
        
        try:
        
                for item1 in response2['items']:
                    data1=dict(channelname = item1['snippet']['channelTitle'],
                                channelid=item1['snippet']['channelId'],
                            video_id=item1["id"],
                            videotitle=item1["snippet"]["title"],
                                videotag=item1['snippet'].get('tags')[0],
                                thumbnail=item1["snippet"]['thumbnails']['default']['url'],
                                description=item1['snippet'].get('description'),
                                publisheddate=item1['snippet']['publishedAt'],
                                duration=item1['contentDetails']['duration'],
                                    views=item1['statistics'].get('viewCount'),
                                    comment=item1['statistics'].get('commentCount'),
                                    likecount=item1['statistics'].get('likeCount'),
                                    favorite=item1['statistics']['favoriteCount'],
                                    definition=item1['contentDetails']['definition'],
                                    caption=item1['contentDetails']['caption']
                                    )
                    videodata.append(data1)

        except:
            pass                            
                     
    return videodata


videodetails=viddata(vd)

def commentdetails(vd):
    commentdata=[]
    try:
        for vid in vd:
            request=youtube.commentThreads().list(
                part='snippet',
                videoId=vid,
                maxResults=50
            )
            response3=request.execute()

            for item in response3['items']:
                data2=dict(commentid=item['snippet']['topLevelComment']['id'],
                           videosid=item['snippet']['topLevelComment']['snippet']['videoId'],
                           commentext=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                           commentauthor=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                           commentpublished=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                commentdata.append(data2)
    except:
        pass
    return commentdata  


get_comment_info=commentdetails(vd)

def playlistdetails(id1):
    nextpagetoken= None
    alldata=[]
    try:
        while True:
            request= youtube.playlists().list(
                part='snippet,contentDetails',
                channelId=id1,
                maxResults=50,
                pageToken=nextpagetoken
            )
            response5=request.execute()


    
            for item in response5['items']:
                data3=dict(playlistid=item['id'],
                        Title=item['snippet']['title'],
                        channelid=item['snippet']['channelId'],
                        channelname=item['snippet']['channelTitle'],
                        publishedat=item['snippet']['publishedAt'],
                        videocount=item['contentDetails']['itemCount'])
                alldata.append(data3)
            nextpagetoken=response5.get('nextPageToken')
            if nextpagetoken is None:
                    break    
    except:
         pass        
    return alldata

get_playlist_info=playlistdetails(id1)


def createtabe():
    db=mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='youtube'

    )
    mycursor= db.cursor()


    cl_list=[]
    for cl_data in get_channel_info:
        cl_list.append(cl_data)
    
    df1= pd.DataFrame(cl_list)
 
    
    engine = create_engine("mysql+mysqlconnector://root:root@localhost/youtube")
    df1.to_sql('channels', con=engine, if_exists='append', index=False)
    db.commit()

def videos ():
    db=mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='youtube'

    )
    mycursor= db.cursor()

    db.commit()
    vl_list=[]
    for vl_data in videodetails:
        vl_list.append(vl_data)
        
    df3= pd.DataFrame(vl_list)
    try:
      df3['duration']=pd.to_timedelta(df3['duration'])
    except:
        pass
    engine = create_engine("mysql+mysqlconnector://root:root@localhost/youtube")
    df3.to_sql('videos', con=engine, if_exists='append', index=False)
    db.commit()


def comment():
    db=mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='youtube'

    )
    mycursor= db.cursor()

    db.commit()
    co_list=[]
    for co_data in get_comment_info:
        co_list.append(co_data)
        
    df4= pd.DataFrame(co_list)

    engine = create_engine("mysql+mysqlconnector://root:root@localhost/youtube")
    df4.to_sql('comment', con=engine, if_exists='append', index=False)
    db.commit()


def playlists():
    db=mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='youtube'

    )
    mycursor= db.cursor()

    db.commit()

    pl_list=[]
    for pl_data in get_playlist_info:
        pl_list.append(pl_data)
        
    df= pd.DataFrame(pl_list)

    
    engine = create_engine("mysql+mysqlconnector://root:root@localhost/youtube")
    df.to_sql('playlists', con=engine, if_exists='append', index=False)
    db.commit()    

def channeldetails():
     get_channel_info =channelinfo(id1)
     vd = videoids(id1)
     videodetails =viddata(vd)
     get_comment_info = commentdetails(vd)
     get_playlist_info =playlistdetails(id1)
     return 'extracted successfully'


def tables():
    createtabe()
    videos()
    comment()
    playlists()
    return 'tables created successfully'


if st.button("collect and store data",type="primary"):
      insert=channeldetails()
      insert1=tables()
      st.success(insert)
      st.success(insert1)
      
      

with st.sidebar:
    st.header("skill take away")
    st.caption("python scripting")
    st.caption("data collection")
    st.caption("api integration")
    st.caption("data management using mysql")




      

