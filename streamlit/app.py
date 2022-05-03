import streamlit as st
import s3fs
import os
import pandas as pd



uploaded_file = st.file_uploader("Choose a file")
if uploaded_file is not None:
  df = pd.read_csv(uploaded_file)


fs = s3fs.S3FileSystem(anon=False)
bytes_to_write = df.to_csv(None).encode()

with fs.open('s3://salonanalytics/myfile.csv', 'wb') as f:
    f.write(bytes_to_write)

