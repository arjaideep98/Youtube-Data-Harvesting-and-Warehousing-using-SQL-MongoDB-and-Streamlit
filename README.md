# Youtube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit
Project 1 – YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit
•	The project is entirely done using Python programming and tools such as Python, SQL and MongoDB is required and should be installed in the system. 
•	The first step in this project is to collect data. To collect YouTube data, Youtube API key is required. API (application programming interface) key is a code used to identify and authenticate an application or user. To create a YouTube API key instructions from this webpage can be followed (https://developers.google.com/youtube/v3/getting-started).
•	Using the YouTube API key the YouTube data such as channel information, Video information and Comments details can be collected. 
•	The collected information is stored in MongoDB. MongoDB is an unstructured SQL database. All the information of each YouTube channel is stored as a file in MongoDB. 
•	From MongoDB the data is migrated to Postgre SQL or MySQL into a Structured data in the form of tables.
•	Till the above step everything is done in back-end which is not visible to users. For the users to interact and get the required data from the collected data, a front-end application is created using Streamlit. 
•	Streamlit is a web-based python code accessible front-end UI (user interface) application. Using this application the collected data is presented to the users. 
•	In each step to use different tools in Python certain library files should be installed in python using ‘pip install’ function. The required library files to be installed for this project are 
from googleapiclient.discovery import build               # to retrieve data from youtube 
import psycopg2                                           # to connect with postgre SQL
import pandas as pd                                       # to use pandas function
from pymongo import MongoClient                           # to connect with MongoDB
import streamlit as st                                                     # to connect with streamlit 
•	While interacting with the Streamlit UI when you give a new channel id, the data is collected and stored in the database and is visible to the user in the webpage. 

