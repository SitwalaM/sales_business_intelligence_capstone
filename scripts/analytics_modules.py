import pandas as pd
from sqlalchemy import create_engine
from fbprophet import Prophet
from fbprophet.serialize import model_to_json, model_from_json
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

 
def update_db_with_data(credentials, dataframe, table_name, dtypes_dictionary):
    ''' function used to connect to update tables in the sql database

    Parameters
    ------
    credentials : dictionary of credentials for connection to the database --> 
    dataframe: pandas datarame to be added to the database
    table_name: string name of table in database
    dtypes_dictionary: dictionary of datatypes for columns in the dataframe
    
    Returns
    -------
    None
    '''

    database_connection = create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(credentials["user"], credentials["passw"], 
                                                      credentials["host"], credentals["database"]), pool_recycle=1, pool_timeout=57600).connect()


    dataframe.to_sql(con=database_connection, 
                     name=table_name, 
                     if_exists='replace',
                     dtype = dtypes_dictionary,
                     chunksize=1000)
    return None

def prepare_for_prophet(customer_data):

    filt_df = customer_data[customer_data["Date"]>"2020-10-1"]
    grouped_day = filt_df.groupby("Date", as_index = False).sum()
    ts = pd.DataFrame({'ds':grouped_day.Date,'y':grouped_day.Total})

    return ts



def forecast_by_Prophet(train, holidays):
    '''
    function that fits data using prophet

    Parameters
    ------
    train: dataframe with columns "ds" and "y" for prophet to fit
    holidays: dataframe of holiday definitions for prophet

    Returns
    ------
    model: trained model object that can be used for fitting 
    '''
    model = Prophet(interval_width=0.95, weekly_seasonality=False, seasonality_mode = 'multiplicative', 
                holidays= holidays, 
                changepoint_range=0.8).  add_seasonality(name="weekly", period= 7, fourier_order= 25)
    model.fit(train)

    return model


def make_forecast(model,refit = False,period=7, train_data = None):
    '''
    function to fit data and make prediction using prophet

    Parameters
    ---
    model: loaded model to use for prediction
    refit: Boolean to tell function whether to refit data
    period: forecast horizon for the prediction
    train_data: provide training data if model will need refitting 

    Returns
    ------
    forecast: dataframe of forecast prediction

    '''
    if refit == True:
        train_data = train_data
        model = forecast_by_Prophet(train_data)
    else:
        pass
    future = model.make_future_dataframe(periods=period)
    forecast = model.predict(future)
    forecast['ds']=pd.to_datetime(forecast['ds'].astype(str))

    return forecast[["ds","yhat","yhat_lower","yhat_upper","trend"]]


def get_rfm_data(data):
    '''
    function that fits data using prophet
    credit: https://practicaldatascience.co.uk/machine-learning/how-to-use-knee-point-detection-in-k-means-clustering

    Parameters
    ------
    data: dataframe with columns customer id: "Customer", date: "Date" and Sales transaction total: "Total"
    
    Returns
    ------
    df_rfm: dataframe containing  
    '''
   
    data['Date']=pd.to_datetime(data['Date'].astype(str), format='%Y-%m-%d')
    data['Total']= data['Total'].astype(float)
    df = data
    end_date = max(df['Date']) + dt.timedelta(days=1)

    df_rfm = df.groupby('Customer').agg(
        recency=('Date', lambda x: (end_date - x.max()).days),
        monetary=('Total', 'sum'))

    df_rfm["frequency"] = df.groupby('Customer').agg({'Date': pd.Series.nunique})

    df_rfm.reset_index()
    df_rfm.sort_values("monetary", ascending = False)

    return df_rfm


def run_kmeans(df, k):
    """Run KMeans clustering, including

    Parameters: 
    ------
    df: rfm pandas dataframe
    k: Number of clusters to assign 

    Returns
    -------
    df: datframe with cluster assigned for each customer ID
    """

    # scale dataframe
    scaler = StandardScaler()
    scaler.fit(df)
    df_norm = scaler.transform(df)
   
    #kmeans clustering
    kmeans = KMeans(n_clusters=k, 
                    random_state=1)
    kmeans.fit(df_norm)


    return df.assign(cluster=kmeans.labels_)


