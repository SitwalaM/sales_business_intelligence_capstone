
from analytics_modules import *
from datetime import timedelta
from datetime import datetime
from datetime import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import s3fs
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 4, 8),
    'email': ['sitwala.mundia@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}


dag = DAG(
    'sales_analytics',
    default_args=default_args,
    description='DAG for twitter monitor',
    schedule_interval = "0 */12 * * *",
)


def extract_data():
    fs = s3fs.S3FileSystem(anon=False)
    filename = str(dt.date.today()) + ".csv"

    def read_file(filename):
        with fs.open(filename) as f:
            return f.read().decode("utf-8")
    file = read_file("s3://salonanalytics/data.csv")
    
    customer_data = pd.read_csv(file)
    customer_data.to_csv(filename)

    return "Data Extracted and Saved to Disk"

def prophet_predict():

    filename = str(dt.date.today()) + ".csv"
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

    output = output.to_csv("pedict_"+file_name)

    return "Prediction Complete and Saved to Disk"

def write_prediction():

    credentials = {"user" : 'root',
                        "passw": '***',
                        "host" :  'localhost',
                        "port" : 3306,
                       " database" : 'salon_analytics'
                }
    dtypes_dictionary = {"ds": sqlalchemy.types.DateTime() ,
                   "yhat": sqlalchemy.types.Numeric,
                    "yhat_lower": sqlalchemy.types.Numeric,
                    "yhat_upper": sqlalchemy.types.Numeric,
                    "trend": sqlalchemy.types.Numeric       
                    }
   
    update_db_with_data(credentials, dataframe, "forecasts",  dtypes_dictionary)

    return "Predictions Updated to DB"



