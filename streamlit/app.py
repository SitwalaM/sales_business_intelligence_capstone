
import streamlit as st
import s3fs
import os
import pandas as pd
from datetime import date
from growth_budget import get_growth_budget
import mysql.connector
from sqlalchemy import create_engine
from app_utilities import *
import plotly.express as px

 
#st.set_page_config(layout="wide")
st.title("Sales Insights")


# Database connection
@st.experimental_singleton
def init_connection():

    return create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(st.secrets["mysql"]["user"], st.secrets["mysql"]["password"], 
                                                      st.secrets["mysql"]["host"], st.secrets["mysql"]["database"]), pool_recycle=1, pool_timeout=57600).connect()
conn = init_connection()


def run_query(query, conn):
  # runs query and returns dataframe

  df = pd.read_sql(query,conn)

  return df


query1 = "SELECT * from forecasts;"
query2 = "SELECT * from transactions;"

#------------ Read dataframes and transformations---------------------------
forecasts_df = run_query(query1,conn)
transactions_df = run_query(query2,conn)
monthly_grouped_sales = group_by_month(transactions_df)
customer_count_per_month = get_customers_per_month(transactions_df)
filtered_forecast = get_forecast_month(forecasts_df,transactions_df)
growth_budget = get_growth_budget(transactions_df)
growth_budget["Net Growth"] = growth_budget.Regained + growth_budget.New - growth_budget.Churned 

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
forecast_month = filtered_forecast.ds.max().strftime(" %B")


#-------------------------- Title and Metrics------------------------------------
#st.markdown('### KPIs')
col1, col2, col3, col4 = st.columns([2,2,2,1.5])
col1.metric(label = "Sales Today", value = f"ZMW {sales_today:,g}", delta = str(round(day_delta*100/sales_yesterday)) + "%")
col2.metric("Sales this Month", value = f"ZMW {month_sales_now:,g}", delta = str(round((month_sales_now - month_sales_last_month)*100/month_sales_last_month )) + "%")
col3.metric("7-Day Sales Forecast:" ,  value = f"ZMW {total_forecast:,g}" )
col4.metric("Customer Count", value = f"{unique_customers_now:,g}", delta = delta_customer_count.item())
st.markdown("""---""")

#-------------------------Upload latest data --------------------------------------
with st.sidebar:
  uploaded_file = st.file_uploader("Upload Latest Sales Data - For Official Use Only", type = ['csv']) 
  if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    fs = s3fs.S3FileSystem(anon=False)
    filename = str(date.today()) + ".csv"
    df.to_csv('s3://salonanalytics/'+filename)

#------------------------Plot the Outputs----------------------------------------------
monthly_sales = px.line(monthly_grouped_sales, x = monthly_grouped_sales.index , y = "Total", title = "Monthly Sales")
customer_count = px.line(customer_count_per_month, x = monthly_grouped_sales.index , y = "Customer", title = "Customer Count")
growth_budget_plot = px.line(growth_budget, x = "months" , y = ["Regained","Churned","New"], title = "Growth Budget")
Net_Growth = px.line(growth_budget, x = "months", y = "Net Growth", title = "Net Growth")


fig1 = st.plotly_chart(monthly_sales, height = 210, use_container_width=True)
fig2 = st.plotly_chart(customer_count, height= 210, use_container_width=True)
fig2 = st.plotly_chart(growth_budget_plot, height= 210, use_container_width=True)
fig2 = st.plotly_chart(Net_Growth, height= 210, use_container_width=True)

