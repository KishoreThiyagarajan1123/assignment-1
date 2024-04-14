import googleapiclient.discovery
import googleapiclient.errors
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st


api = "AIzaSyAevIqkfz_T0toWK4J_PkdmhN0tQLzM-x4"
id1 = "UCJcCB-QYPIBcbKcBQOTwhiA"


api_service_name = "youtube"
api_version = "v3"



youtube = googleapiclient.discovery.build(
api_service_name, api_version, developerKey=api)

def channelinfo(id1):
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
  
    return data


get_channel_info = channelinfo(id1)


df1=pd.DataFrame(get_channel_info,index=[0])

def videoids (id1):
        vidids =[]
        vidinfo = youtube.channels().list(
                part ="contentDetails",
                id =id1).execute()
        playlistid = vidinfo['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        pagetoken=None

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
        return vidids  

vd = videoids(id1)   


def viddata(vd):
    videodata=[]
    for vi in vd:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=vi
        )
        response2=request.execute()
        

        
        for item1 in response2['items']:
            data1=dict(video_id=item1["id"],
                    videotitle=item1["snippet"]["title"],
                    videotag=item1['snippet'].get('tags')[0],
                        thumbnail=item1["snippet"]['thumbnails']['default']['url'],
                        description=item1['snippet'].get('description'),
                        publisheddate=item1['snippet']['publishedAt'],
                        duration=item1['contentDetails']['duration'],
                            views=item1['statistics'].get('viewCount'),
                            comment=item1['statistics'].get('commentCount'),
                            favorite=item1['statistics']['favoriteCount'],
                            definition=item1['contentDetails']['definition'],
                            caption=item1['contentDetails']['caption']
                            )
            videodata.append(data1)
    return videodata


videodetails=viddata(vd)


df3=pd.DataFrame(videodetails)



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


get_comment_info = commentdetails(vd)

df4=pd.DataFrame(get_comment_info)

def playlistdetails(id1):
    nextpagetoken= None
    alldata=[]
    while True:
        request= youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=id1,
            maxResults=50,
            pageToken=nextpagetoken
        )
        response4=request.execute()

        for item in response4['items']:
            data=dict(playlistid=item['id'],
                      Title=item['snippet']['title'],
                      channelid=item['snippet']['channelId'],
                      channelname=item['snippet']['channelTitle'],
                      publishedat=item['snippet']['publishedAt'],
                      videocount=item['contentDetails']['itemCount'])
            alldata.append(data)
        nextpagetoken=response4.get('nextPageToken')
        if nextpagetoken is None:
               break
    return alldata

get_playlist_info=playlistdetails(id1)

df=pd.DataFrame(get_playlist_info)


def channeldetails(id1):
    ch_details = get_channel_info 
    pl_details = get_playlist_info
    vi_detail = videodetails
    com_details = get_comment_info


def createtabe():
    db=mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='youtube'

    )
    mycursor= db.cursor()



    df1=pd.DataFrame(get_channel_info,index=[0])

    createquery = '''create table if not exists channel3(channelname varchar(100),
                                                    channelid varchar(80),
                                                    subscriber bigint,
                                                    views bigint,
                                                    totalvideo int,
                                                    channeldescription text,
                                                    playlistid varchar(80))'''
    mycursor.execute(createquery)   
    db.commit()



    for index,row in df1.iterrows():
        insertquery = '''insert into channel3(channelname,
                                            channelid,
                                            subscriber,
                                            views,
                                            totalvideo	,
                                            channeldescription,
                                            playlistid)
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channelname'],
                row['channelid'],
                row['subscriber'],
                row['views'],
                    row['totalvideo'],
                    row['channeldescription'],
                    row['playlistid'],)    
        
    mycursor.execute(insertquery,values)   
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

    df=pd.DataFrame(get_playlist_info)
    engine = create_engine("mysql+mysqlconnector://root:root@localhost/youtube")
    df.to_sql('playlist', con=engine, if_exists='append', index=False)
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

    df4=pd.DataFrame(get_comment_info)

    engine = create_engine("mysql+mysqlconnector://root:root@localhost/youtube")
    df4.to_sql('comments', con=engine, if_exists='append', index=False)
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

    df3=pd.DataFrame(videodetails)

    engine = create_engine("mysql+mysqlconnector://root:root@localhost/youtube")
    df3.to_sql('video', con=engine, if_exists='append', index=False)
    db.commit()


def tables():
    createtabe()
    playlists()
    comment()
    videos()
    return 'tables created successfully' 


Tables = tables()

def channel_table():
    df1=st.dataframe(get_channel_info,index=[0])
    return df1

def playlist_table():
   df= st.dataframe(get_playlist_info)
   return df

def comment_table():
    df4=st.dataframe(get_comment_info)
    return df4

def video_table():
    df3=st.dataframe(videodetails)
    return df3

with st.sidebar:
    st.title(":red[youtube data haversting and warehousing]")
    st.header("skill take away")
    st.caption("python scripting")
    st.caption("data collection")
    st.caption("api integration")
    st.caption("data management using mysql")

id1 = st.text_input("enter the channel id")

if st.button("collect and store data"):
    ch_id=[]
    ch_id.append(get_channel_info['channelid'])
    if id1 in ch_id:
        st.success('channel details of the given channel id already exists')
    else:
        insert= channeldetails(id1)
        st.success(insert)   


if st.button('migrate to MySQL'):
    Tables=tables()
    st.success(Tables)

showtable = st.radio('select the table for view',('channel3','playlist','comments','video'))

if showtable=='channel3':
    createtabe()
elif showtable == 'playlist':
    playlist_table()
elif showtable == 'comments':
    comment_table()
elif showtable == 'video':
    video_table()


db=mysql.connector.connect(
host='localhost',
user='root',
password='root',
database='youtube'
)
mycursor= db.cursor()

question = st.selectbox("select your question",('1.What are the names of all the videos and their corresponding channels?',
                                                '2.Which channels have the most number of videos, and how many videos do they have?',
                                                '3.What are the top 10 most viewed videos and their respective channels?',
                                                '4.How many comments were made on each video, and what are their corresponding video names?',
                                                '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
                                                '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                                '7.What is the total number of views for each channel and what are their corresponding channel names?',
                                                '8.What are the names of all the channels that have published videos in the year?',
                                                '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                                '10.Which videos have the highest number of comments, and what are their corresponding channel names?'))






db=mysql.connector.connect(
host='localhost',
user='root',
password='root',
database='youtube'
)
mycursor= db.cursor()

if question =='1.What are the names of all the videos and their corresponding channels?':
        query1 = '''select videotitle,channalname from video1'''
        mycursor.execute(query1)
        t1=mycursor.fetchall()
        df5 = pd.DataFrame(t1,columns=["video title","channel name"])
        st.write(df5)
elif question =='2.Which channels have the most number of videos, and how many videos do they have?':
                query2 = '''select channelname,totalvideo from channel3
                        order by totalvideo desc'''
                mycursor.execute(query2)
                t2=mycursor.fetchall()
                df6 = pd.DataFrame(t2,columns=["channel name","total video"])
                st.write(df6)     
elif question =='3.What are the top 10 most viewed videos and their respective channels?':
        query3 = '''select views,channalname,videotitle from video1
                     where views is not null order by views desc limit 10'''
        mycursor.execute(query3)
        t3=mycursor.fetchall()
        df7 = pd.DataFrame(t3,columns=["views","channel name","video title"])
        st.write(df7)     
elif question =='4.How many comments were made on each video, and what are their corresponding video names?':
        query4 = '''select comment,videotitle from video1
                     where views is not null '''
        mycursor.execute(query4)
        t4=mycursor.fetchall()
        df8 = pd.DataFrame(t4,columns=["no of  comments","video title"])
        st.write(df8)
elif question =='5.Which videos have the highest number of likes, and what are their corresponding channel names?':
        query5 = '''select videotitle,channalname,likecount from video1    
                    where likecount is not null order by likecount desc'''
        mycursor.execute(query5)
        t5=mycursor.fetchall()
        df9 = pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
        st.write(df9)         
elif question =='6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        query6 = '''select likecount,videotitle from video1 '''
        mycursor.execute(query6)
        t6=mycursor.fetchall()
        df10 = pd.DataFrame(t6,columns=["like","videotitle"])
        st.write(df10) 
elif question =='7.What is the total number of views for each channel and what are their corresponding channel names?':
        query7 = '''select channelname,views as totalviews from channel3 '''
        mycursor.execute(query7)
        t7=mycursor.fetchall()
        df11 = pd.DataFrame(t7,columns=["channel3","totalviews"])
        st.write(df11) 
elif question =='8.What are the names of all the channels that have published videos in the year?':
        query8 = '''select videotitle,publisheddate,channalname as channelname from video1 '''
        mycursor.execute(query8)
        t8=mycursor.fetchall()
        df12 = pd.DataFrame(t8,columns=["videotitle","publisheddate","channelname"])
        st.write(df12) 
elif question =='9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        query9 = '''select channalname as channelname,AVG(duration) as averageduration from video1 group by channalname'''
        mycursor.execute(query9)
        t9=mycursor.fetchall()
        df13 = pd.DataFrame(t9,columns=["channelname","averageduration"])
        t9=[]
        for index,row in df13.iterrows():
                channel_title=row['channelname']
                average_duration=row['averageduration']
                average_duration_str=str(average_duration)
                t9.append(dict(channeltitle=channel_title,averageduration=average_duration_str))
        df15= pd.DataFrame(t9)       
elif question =='10.Which videos have the highest number of comments, and what are their corresponding channel names?':
        query10 = '''select videotitle,channalname as channelname , comment from video1 where comment is not null order by comment desc '''
        mycursor.execute(query10)
        t10=mycursor.fetchall()
        df14 = pd.DataFrame(t10,columns=["videotitle","channelname","comment"])
        st.write(df14) 




