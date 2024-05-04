import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st



db=mysql.connector.connect(
host='localhost',
user='root',
password='root',
database='youtube'
)
mycursor= db.cursor()

question = st.selectbox("select your question",('select question','1.What are the names of all the videos and their corresponding channels?',
                                                '2.Which channels have the most number of videos, and how many videos do they have?',
                                                '3.What are the top 10 most viewed videos and their respective channels?',
                                                '4.How many comments were made on each video, and what are their corresponding video names?',
                                                '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
                                                '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                                '7.What is the total number of views for each channel and what are their corresponding channel names?',
                                                '8.What are the names of all the channels that have published videos in the year of 2022?',
                                                '9.What is the  duration of all videos in each channel, and what are their corresponding channel names?',
                                                '10.Which videos have the highest number of comments, and what are their corresponding channel names?'))

    






db=mysql.connector.connect(
host='localhost',
user='root',
password='root',
database='youtube'
)
mycursor= db.cursor()

if question =='1.What are the names of all the videos and their corresponding channels?':
    query1 = '''select videotitle,channelname from videos'''
    mycursor.execute(query1)
    t1=mycursor.fetchall()
    df5 = pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df5)
elif question =='2.Which channels have the most number of videos, and how many videos do they have?':
            query2 = '''select channelname,totalvideo from channels
                    order by totalvideo desc'''
            mycursor.execute(query2)
            t2=mycursor.fetchall()
            df6 = pd.DataFrame(t2,columns=["channel name","total video"])
            st.write(df6)     
elif question =='3.What are the top 10 most viewed videos and their respective channels?':
    query3 = '''select views,channelname,videotitle from videos
                where views is not null order by views desc limit 10'''
    mycursor.execute(query3)
    t3=mycursor.fetchall()
    df7 = pd.DataFrame(t3,columns=["views","channel name","video title"])
    st.write(df7)     
elif question =='4.How many comments were made on each video, and what are their corresponding video names?':
    query4 = '''select comment,videotitle from videos
                where views is not null '''
    mycursor.execute(query4)
    t4=mycursor.fetchall()
    df8 = pd.DataFrame(t4,columns=["no of  comments","video title"])
    st.write(df8)
elif question =='5.Which videos have the highest number of likes, and what are their corresponding channel names?':
    query5 = '''select videotitle,channelname,likecount from videos    
                where likecount is not null order by likecount desc'''
    mycursor.execute(query5)
    t5=mycursor.fetchall()
    df9 = pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df9)         
elif question =='6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    query6 = '''select likecount,videotitle from videos '''
    mycursor.execute(query6)
    t6=mycursor.fetchall()
    df10 = pd.DataFrame(t6,columns=["like","videotitle"])
    st.write(df10) 
elif question =='7.What is the total number of views for each channel and what are their corresponding channel names?':
    query7 = '''select channelname,views as totalviews from channels '''
    mycursor.execute(query7)
    t7=mycursor.fetchall()
    df11 = pd.DataFrame(t7,columns=["channel3","totalviews"])
    st.write(df11) 
elif question =='8.What are the names of all the channels that have published videos in the year of 2022?':
    query8 = '''select videotitle,publisheddate, channelname from videos 
                 where extract(year from publisheddate)=2022'''
    mycursor.execute(query8)
    t8=mycursor.fetchall()
    df12 = pd.DataFrame(t8,columns=["videotitle","publisheddate","channelname"])
    st.write(df12) 
elif question =='9.What is the  duration of all videos in each channel, and what are their corresponding channel names?':
    query9 = '''select channelname as channelname,AVG(duration) as averageduration from videos group by channelname'''
    mycursor.execute(query9)
    t9=mycursor.fetchall()
    df13 = pd.DataFrame(t9,columns=["channelname","averageduration"])
    d9=[]
    for index,row in df13.iterrows():
            channel_title=row['channelname']
            average_duration=row['averageduration']
            average_duration_str=str(average_duration)
            d9.append(dict(channeltitle=channel_title,averageduration=average_duration_str))
    df15= pd.DataFrame(d9)  
    st.write(df15)
elif question =='10.Which videos have the highest number of comments, and what are their corresponding channel names?':
    query10 = '''select videotitle, channelname , comment from videos where comment is not null order by comment desc '''
    mycursor.execute(query10)
    t10=mycursor.fetchall()
    df14 = pd.DataFrame(t10,columns=["videotitle","channelname","comment"])
    st.write(df14) 


     
