mport pandas as pd
from sqlalchemy import create_engine
from fbprophet import Prophet
from fbprophet.serialize import model_to_json, model_from_json

 
def update_db_with_data(credentials, dataframe, table_name, dtypes_dictionary):
    ''' function used to connect to update tables in the sql database
    inputs
    ------
    credentials : dictionary of credentials for connection to the database --> 
    dataframe: pandas datarame to be added to the database
    table_name: string name of table in database
    dtypes_dictionary: dictionary of datatypes for columns in the dataframe
    
    outputs
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


def forecast_by_Prophet(train, holidays = zambia_holidays):
    '''
    function that fits data using prophet
    inputs
    ------
    train: dataframe with columns "ds" and "y" for prophet to fit
    holidays: dataframe of holiday definitions for prophet

    return
    ------
    model: trained model object that can be used for fitting 
    '''
    model = Prophet(interval_width=0.95, weekly_seasonality=False, seasonality_mode = 'multiplicative', 
                holidays= holidays, 
                changepoint_range=0.8).add_seasonality(name="weekly", period= 7, fourier_order= 25)
    model.fit(train)

    return model


def make_forecast(model,refit = False,period=7, train_data = None):
    '''
    function to fit data and make prediction using prophet

    input
    ---
    model: loaded model to use for prediction
    refit: Boolean to tell function whether to refit data
    period: forecast horizon for the prediction
    train_data: provide training data if model will need refitting 

    output
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
    return forecast
