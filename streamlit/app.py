import streamlit as st
import s3fs
import os
import pandas as pd
from datetime import date
import mysql.connector
from sqlalchemy import create_engine
 

uploaded_file = st.file_uploader("Upload Latest Sales Data", type = ['csv']) 
if uploaded_file is not None:
  df = pd.read_csv(uploaded_file)


  fs = s3fs.S3FileSystem(anon=False)
  filename = str(date.today()) + ".csv"
  df.to_csv('s3://salonanalytics/'+filename)


@st.experimental_singleton
def init_connection():
    return create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(st.secrets["mysql"]["user"], st.secrets["mysql"]["password"], 
                                                      st.secrets["mysql"]["host"], st.secrets["mysql"]["database"]), pool_recycle=1, pool_timeout=57600).connect()

conn = init_connection()

query = "SELECT * from forecasts;"

forecasts_df = pd.read_sql(query, conn)
st.dataframe(result_dataFrame)