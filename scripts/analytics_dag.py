
from analytics_modules import *
from datetime import timedelta
from datetime import datetime
from datetime import datetime as dt
import sqlalchemy
from datetime import date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import configparser
import s3fs
import os
import sys

sys.path.append('../../')
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 4),
    'email': ['sitwala.mundia@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}


dag = DAG(
    'sales_analytics',
    default_args=default_args,
    description='DAG sales analyrics',
    schedule_interval = "@daily",
  
)


def extract_data():
    # dag Python operator to extract raw customer data
    
    config = configparser.ConfigParser()
    config.read("./credentials.ini")
    key = config["aws"]["AWS_ACCESS_KEY_ID"]
    secret = config["aws"]["AWS_SECRET_ACCESS_KEY"]
    s3 = s3fs.S3FileSystem(anon=False, key=key, secret=secret)

    filename = str(date.today()) + ".csv"
    customer_data = pd.read_csv('s3://salonanalytics/'+filename)

    # fixes error in initial dataset
    customer_data['Date']=pd.to_datetime(customer_data['Date'].astype(str))
    customer_data = customer_data[customer_data['Date']> "2020-09-30"]
    #customer_data = customer_data[customer_data['Date']< "2021-11-15"]
    customer_data.to_csv(filename)      # save to file

    return "Data Extracted and Saved to Disk"

def prophet_predict():
    # dag Python operator to run forecast 

    filename = str(date.today()) + ".csv"
    customer_data = pd.read_csv(filename)
    ts = prepare_for_prophet(customer_data)

    # Define Zambian holidays to help the model lear customer behaviour during holidays

    zambia_holidays = pd.DataFrame({
    'holiday': 'general',
    'ds': pd.to_datetime(['2021-01-01', '2021-03-12', '2021-04-10',
                            '2021-04-11', '2021-04-13', '2021-05-01',
                            '2021-07-07', '2021-08-03', '2021-10-19',
                            '2020-10-19', '2021-10-24', '2020-12-25']),
    'lower_window': 0,
    'upper_window': 1,
    })

    model = forecast_by_Prophet(ts, zambia_holidays)
    output = make_forecast(model,refit = False,period=7, train_data = None)

    output = output.to_csv("pedict_"+filename)

    return "Prediction Complete and Saved to Disk"

def write_prediction():
    # dag python operator that writes the data to database

    global credentials
    
    credentials = {"user" : 'remote',
                        "passw": '{}',
                        "host" :  '{}',
                        "port" : 3306,
                       "database" : 'salon_analytics'
                }
    dtypes_dictionary = {"ds": sqlalchemy.types.DateTime() ,
                   "yhat": sqlalchemy.types.Numeric,
                    "yhat_lower": sqlalchemy.types.Numeric,
                    "yhat_upper": sqlalchemy.types.Numeric,
                    "trend": sqlalchemy.types.Numeric       
                    }
    
    filename = str(date.today()) + ".csv"
    dataframe = pd.read_csv("pedict_"+filename)
    update_db_with_data(credentials, dataframe, "forecasts",  dtypes_dictionary)

    return "Predictions Updated to DB"

def write_transactions():
    # dag python operator that writes the data to database

    credentials = {"user" : 'remote',
                        "passw": '{}',
                        "host" :  '{}',
                        "port" : 3306,
                       "database" : 'salon_analytics'
                }
    dtypes_dictionary = {"Date": sqlalchemy.types.DateTime() ,
                        "Customer": sqlalchemy.types.VARCHAR(length=255),
                        "Total": sqlalchemy.types.Numeric
                         }
    
    file = str(date.today()) + ".csv"
    dataframe = pd.read_csv(file)
    update_db_with_data(credentials, dataframe, "transactions",  dtypes_dictionary)
    
    return  "Transactions Updated to DB"


# python operators for the pipeline are defined below
extract = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

model = PythonOperator(
    task_id='predictions',
    python_callable=prophet_predict,
    dag=dag,
)

update_db_predictions = PythonOperator(
    task_id='write_predictions',
    python_callable=write_prediction,
    dag=dag,
)

update_db_transactions = PythonOperator(
    task_id='write_transactions',
    python_callable=write_transactions,
    dag=dag,
)

# define pipeline and dependencies here
extract >> update_db_transactions >> model >> update_db_predictions
