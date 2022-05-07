import pandas as pd
from datetime import datetime, timedelta



def group_by_month(df):

    df = df.sort_values("Date", ascending = True)
    df["month-year"] =  df["Date"].apply(lambda x: x.strftime('%Y-%m')) 
    #df = df.groupby("month-year", as_index = False).sum()
    df = pd.pivot_table(df, index = 'month-year', values = 'Total', aggfunc='sum')

    return df

    
def get_date_references(df):

    today =df.Date.max() #datetime.strptime(df.Date.max(), '%Y-%m-%d %H:%M:%S')
    yesterday = today  - timedelta(days=1)

    return {"today": today.strftime('%Y-%m-%d'), "yesterday": yesterday.strftime('%Y-%m-%d')}


def get_total_day(df,day):

    df = df.copy()
    df["Date"] =  df["Date"].apply(lambda x: x.strftime('%Y-%m-%d')) 
    df = df[df["Date"]==day]
    sales = df.Total.sum()

    return sales


def get_customers_per_month(df):

    df = df.copy()
    df["Date"] =  df["Date"].apply(lambda x: x.strftime('%Y-%m')) 
    df = pd.pivot_table(df, index = 'Date', values = 'Customer', aggfunc='nunique')

    return df


def get_forecast_total(df, last_day):

    df = df.copy()
    


    return forecast_total

