import pandas as pd
from datetime import datetime, timedelta
import calendar
from dateutil.relativedelta import *



def group_by_month(df):
    '''
    takes transaction dataframe and groups by month

    Parameters
    ----------
    df: pandas dataframe of transactions

    Returns
    ---------
    df: pandas dataframe pivoted by the month/year
    '''

    df = df.sort_values("Date", ascending = True)
    df["month-year"] =  df["Date"].apply(lambda x: x.strftime('%Y-%m')) 
    df = pd.pivot_table(df, index = 'month-year', values = 'Total', aggfunc='sum')

    return df

    
def get_date_references(df):
    '''
    Returns the current date and date yesterday based on latest queried data

    Parameters
    ----------
    df: pandas dataframe of transactions

    Returns
    --------
    dictionary >> {"today": date, "yesterday": date}

    '''

    today =df.Date.max() #datetime.strptime(df.Date.max(), '%Y-%m-%d %H:%M:%S')
    yesterday = today  - timedelta(days=1)

    return {"today": today.strftime('%Y-%m-%d'), "yesterday": yesterday.strftime('%Y-%m-%d')}


def get_total_day(df,day):
    '''
    gets the total sales for a particular dat

    Parameters
    ----------
    df: pandas dataframe of transactions

    day: string format date

    Returns
    --------
    sales: float - total sales for day
    '''

    df = df.copy()
    df["Date"] =  df["Date"].apply(lambda x: x.strftime('%Y-%m-%d')) 
    df = df[df["Date"]==day]
    sales = df.Total.sum()

    return sales


def get_customers_per_month(df):
    '''
    counts number of customers per month

    Parameters
    ----------
    df: pandas dataframe of transactions

    Returns
    -------
    df: pandas dataframe pivoted by month/year and number of unique customers
    '''

    df = df.copy()
    df["Date"] =  df["Date"].apply(lambda x: x.strftime('%Y-%m')) 
    df = pd.pivot_table(df, index = 'Date', values = 'Customer', aggfunc='nunique')

    return df


def get_forecast_month(df_forecast,df_transactions):
    '''
    filters predictions dataframe to only show pending days in the month or
    if it's the end of the month, show predictions for next month

    Parameters
    ----------
    df_forecast: pandas dataframe output from Prophet model

    df_transactions: transactions dataframe

    Returns
    ---------
    df_future: pandas dataframe of filtered Prophet model
    '''

    df_future = df_forecast.copy()
    df_future["ds"] = df_future["ds"].apply(lambda x:  datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S'))
    max_date = df_transactions.loc[df_transactions.index[-1],"Date"]

    if max_date.day ==  calendar.monthrange(max_date.year, max_date.month)[1]:
        max_filter =  max_date + relativedelta(months=+1)
    else:
        days_delta = calendar.monthrange(max_date.year, max_date.month)[1] -  max_date.day
        max_filter = max_date + relativedelta(days=days_delta)

    df_future = df_future[(df_future["ds"] <= max_filter) & (df_future["ds"] > max_date)]
        

    return df_future

