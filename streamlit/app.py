import streamlit as st
import s3fs
import os
import pandas as pd
from datetime import date
 

uploaded_file = st.file_uploader("Choose a file", type = ['csv']) 
if uploaded_file is not None:
  df = pd.read_csv(uploaded_file)


  fs = s3fs.S3FileSystem(anon=False)
  filename = str(date.today()) + ".csv"
  df.to_csv('s3://salonanalytics/'+filename)

