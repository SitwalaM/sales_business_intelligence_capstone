import streamlit as st
import s3fs
import os
import pandas as pd
from datetime import date
import mysql.connector
from sqlalchemy import create_engine
from app_utilities import *
import plotly.express as px
 

st.title("Sales Dashboard")

# Database connection
@st.experimental_singleton
def init_connection():

    return create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(st.secrets["mysql"]["user"], st.secrets["mysql"]["password"], 
                                                      st.secrets["mysql"]["host"], st.secrets["mysql"]["database"]), pool_recycle=1, pool_timeout=57600).connect()
conn = init_connection()

#@st.experimental_memo(ttl=600)
def run_query(query, conn):
  # runs query and returns dataframe

  df = pd.read_sql(query,conn)

  return df


query1 = "SELECT * from forecasts;"
query2 = "SELECT * from transactions;"

#------------ Read in main dataframes and transformations---------------------------
forecasts_df = run_query(query1,conn)
transactions_df = run_query(query2,conn)
monthly_grouped_sales = group_by_month(transactions_df)
customer_count_per_month = get_customers_per_month(transactions_df)
filtered_forecast = get_forecast_month(forecasts_df,transactions_df)


#------------------------ Sales Metrics-----------------------------------------------
dates = get_date_references(transactions_df)
sales_today = get_total_day(transactions_df ,dates["today"])
sales_yesterday = get_total_day(transactions_df ,dates["yesterday"])
day_delta = sales_today - sales_yesterday
month_sales_now = monthly_grouped_sales.iloc[-1,0]
month_sales_last_month = monthly_grouped_sales.iloc[-2,0]
unique_customers_now = customer_count_per_month.iloc[-1,0]
unique_customers_last_month = customer_count_per_month.iloc[-2,0]
delta_customer_count =  unique_customers_now - unique_customers_last_month 
total_forecast = filtered_forecast.yhat.sum()
forecast_month = filtered_forecast.ds.max().strftime("%b")

#-------------------------- Title and Metrics------------------------------------
st.markdown('### KPIs')
col1, col2, col3, col4 = st.columns(4)
col1.metric(label = "Sales Today", value = "K "+ f"{sales_today:,}", delta = day_delta)
col2.metric("Sales this Month", value = f"{month_sales_now:,}", delta = month_sales_now - month_sales_last_month )
col3.metric("Sales Forecast:  " +  f"{forecast_month:,}", value = total_forecast )
col4.metric("Customer Count", value = f"{unique_customers_now:,}", delta = delta_customer_count.item())



#-------------------------Upload latest data --------------------------------------
with st.sidebar:
  uploaded_file = st.file_uploader("Upload Latest Sales Data - For Official Use Only", type = ['csv']) 
  if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    fs = s3fs.S3FileSystem(anon=False)
    filename = str(date.today()) + ".csv"
    df.to_csv('s3://salonanalytics/'+filename)


monthly_sales = px.line(monthly_grouped_sales, x = monthly_grouped_sales.index , y = "Total")
st.plotly_chart(monthly_sales)

customer_count = px.line(customer_count_per_month, x = monthly_grouped_sales.index , y = "Customer")
st.plotly_chart(customer_count)
#st.write(t)
