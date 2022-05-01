
import pandas as pd
from sqlalchemy import create_engine

 
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