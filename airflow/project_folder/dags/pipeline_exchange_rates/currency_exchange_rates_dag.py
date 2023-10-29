#Standard imports
import json
import pathlib
import datetime 
import numpy as np
import pandas as pd
import logging

#Airflow
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres     import PostgresHook

from airflow.operators.python        import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.sensors.http_sensor     import HttpSensor
from airflow.models                  import Variable
import airflow.utils.dates

#API/HHTP
from requests.compat import urljoin

#Custom imports
from   includes.db_models.db_currency_exchange import source_data_EUR,source_data_USD
import includes.db_postgres.postgres as postgresdb 

#Constant defintions
AIRFLOW_CONN_ID_DB_CURRENCY        = "postgres_db_currency_exchange"      
AIRFLOW_CONN_ID_API_OPENRAPI_COM   = "api_endpoint_openerapi_com_v6" #Connection ID as specified with airflow engine. Go to admin | connections. 

BASE_CURRENCY_CODE   = "USD"
TARGET_CURRENCY_CODE = "EUR"

#Read airflow variable to establish whether we should by default log responses to log file
log_response = True if Variable.get("environment",True) != 'production' else False


#------------------------------------------------------------------------------------------------------------
# HELPER FUNCIONS
#------------------------------------------------------------------------------------------------------------

def verify_curr_exchange_rates_endpoint_response(response):
    '''
    Verifies whether the request to the rest endpoint was successful or not
    by checking the result within the response message
    response: A JSON string encompassing details of the API call.
    '''
    if response and response.json()["result"] == "success":
        return True
    else:
        return False

def verify_api_response_by_http_status_code(response):
    if response and response.status_code == 200:
        return True
    else:
        return False

def _get_latest_curr_exchange_rates_endpoint_param(curr_code):
    '''
    Function that builds the request in order to get the latest currency exchange rates.
    An API key is not required as the endpoint in publicly available.

    curr_code: An ISO 4217 Three Letter Currency Code 
    Reference: https://www.exchangerate-api.com/docs/pair-conversion-requests
    Returns: service endpoint and the currency code, delimted.
    '''
    SERVICE_ENDPOINT = "latest"

    if not isinstance(curr_code,str) and len(curr_code) == 3:
        raise ValueError('Invalid value specified: {}'.format(curr_code))
    
    return "/".join([SERVICE_ENDPOINT,curr_code])

def _get_conversion_curr_exchange_rates_endpoint_param(api_key,base_curr_code,target_curr_code,amount=None):
    '''
    Function that builds the request in order to perform currency conversion from one currency code to another.
    A private an API key is required to be included in the request.

    api_key: Private service API key
    base__curr_code: Source currency code. Must comply with ISO 4217 Three Letter Currency Code standard
    target_curr_code:    Target currency code. Must comply with ISO 4217 Three Letter Currency Code standard
    Reference: https://www.exchangerate-api.com/docs/pair-conversion-requests
    Returns: service endpoint and the currency code, delimted.
    '''
    SERVICE_ENDPOINT="pair"

    if not isinstance(base_curr_code,str) and len(base_curr_code) == 3:
        raise ValueError('Invalid value specified: {}'.format(base_curr_code))
    
    if not isinstance(target_curr_code,str) and len(target_curr_code) == 3:
        raise ValueError('Invalid value specified: {}'.format(target_curr_code))

    if amount and not (isinstance(amount,int) or isinstance(amount,float)):
        raise ValueError('Invalid value specified: {}'.format(amount))


    base_curr_code      = base_curr_code.upper()
    target_curr_code    = target_curr_code.upper()

    base_endpoint = "/".join([api_key,SERVICE_ENDPOINT,base_curr_code,target_curr_code])
    
    if amount: 
        base_endpoint = "/".join([base_endpoint,str(amount)])

    return base_endpoint

#------------------------------------------------------------------------------------------------------------
#DEFINITION OF DAG
#------------------------------------------------------------------------------------------------------------
#Ref: https://airflow.apache.org/docs/apache-airflow/1.10.13/_api/airflow/operators/index.html
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    #'email_on_failure': False,
    #'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=10)
}

#The data provider has currency exchange rate data refreshed daily.
#These values are defined and can be noted in time_last_update_utc / time_next_update_utc JSON keys.

currency_exchange_rates_dag = DAG(
    dag_id       = "retrieve_currency_exchange_rates_dag",
    description  = "Downloads the latest exchange currency rates and populated database table",
    start_date   = airflow.utils.dates.days_ago(30), 
    schedule     ="@daily",                      #TO DO verify schedule interval of the DAG
    catchup      = False,
    default_args=  default_args
)

#------------------------------------------------------------------------------------------------------------
#TASK 1 - VERIFY WHETHER ENDPOINT IS AVAILABLE
#------------------------------------------------------------------------------------------------------------
task_check_endpoint_availavility = HttpSensor(
            task_id      = 'verify_endpoint_availablity',
            http_conn_id = AIRFLOW_CONN_ID_API_OPENRAPI_COM,
            dag          = currency_exchange_rates_dag,
            endpoint     = ''
        )
task_check_endpoint_availavility.log_response   = log_response
task_check_endpoint_availavility.endpoint       = _get_latest_curr_exchange_rates_endpoint_param(BASE_CURRENCY_CODE)


#------------------------------------------------------------------------------------------------------------
#TASK 2 - RETRIEVAL OF CURRENCY EXCHANGE RATES VIA HTTP ENDPOINT
#------------------------------------------------------------------------------------------------------------
#example url: https://open.er-api.com/v6/latest/USD
task_http_get_currency_exchange_rates=SimpleHttpOperator(
            task_id     = 'retrieve_currency_exchange_rates',
            method      = 'GET',
            http_conn_id= AIRFLOW_CONN_ID_API_OPENRAPI_COM,
            headers     = {"Content-Type": "application/json"},
            dag         = currency_exchange_rates_dag,  
    )
task_http_get_currency_exchange_rates.log_response   = log_response
task_http_get_currency_exchange_rates.endpoint       = _get_latest_curr_exchange_rates_endpoint_param(BASE_CURRENCY_CODE)
task_http_get_currency_exchange_rates.response_check = lambda response : verify_curr_exchange_rates_endpoint_response(response)

#------------------------------------------------------------------------------------------------------------
#TASK 3 - DATA CLEANSING
#------------------------------------------------------------------------------------------------------------
def _data_processor_cleansing(ti):
    '''
    Stub function that performs data cleansing prior to the application of data transformations
    '''
    pass

task_response_data_processor_cleansing = PythonOperator(
            task_id='perform_data_cleansing',
            dag    = currency_exchange_rates_dag,  
            python_callable= _data_processor_cleansing
    )
task_response_data_processor_cleansing.log_response   = log_response

#------------------------------------------------------------------------------------------------------------
#TASK 4 - DATA TRANSFORMATION
#------------------------------------------------------------------------------------------------------------

def _convert_base_currency(curr_rates:dict,target_curr_code:str):
    '''
    Converts a currency exchange table table based on a base currency code to another.
    This entails simply dividing the existing currency rate by the value of the target 
    currency code
    curr_rates: A dict data structure holding the currency rates retrieved from the API call
    target_curr_code: The target currency code based on xxxx standard 
    
    Returns: A dataframe incuding the orginal currency rates and the computed rates.
    '''
    #Create a dataframe from dict instance
    df_curr_dates = pd.DataFrame.from_dict(curr_rates,orient='index',dtype=float)
    df_curr_dates.columns=['base_rate']
    
    #Verify the target currency code is found in the currency rate table
    try:
        target_curr_rate = df_curr_dates.loc[target_curr_code]
    except KeyError as e:
        raise Exception('Target currency code was not found in the currency exchange table',e)

    #Perform the conversion 
    df_curr_dates['target_rate'] = df_curr_dates['base_rate'] / df_curr_dates.loc[target_curr_code].values[0]
    
    df_curr_dates.reset_index(drop=False,inplace=True)
    df_curr_dates = df_curr_dates.drop(columns="base_rate")
    df_curr_dates = df_curr_dates.rename(columns={"target_rate":"rate","index":"currency"})
    
    return df_curr_dates

def _data_processor_transformation(ti):
    '''
    Performs any data processing on the data received from the API endpoint. 
    In this case, the currency table needs to be recomputed so that the base currency is change to the target currency code. 
    '''
    json_response   = ti.xcom_pull(task_ids=["retrieve_currency_exchange_rates"]) #retrieve msgs sent between tasks via XCOM.
    
    json_response   = json.loads(json_response[0])
    base_curr_code  = json_response['base_code'] #get the expected base currencty code 
    dict_curr_rates = json_response['rates']     #get the currency rates

    #1. Perform change of base currency code to the rate table
    df_curr_table   = _convert_base_currency(dict_curr_rates,TARGET_CURRENCY_CODE)
    
    return {'df_curr_table':df_curr_table, 'json_response':json_response} 


task_response_data_processor_transformer = PythonOperator(
            task_id ='perform_data_transformation',
            dag     = currency_exchange_rates_dag,  
            python_callable = _data_processor_transformation
    )
task_response_data_processor_transformer.log_response   = log_response

#------------------------------------------------------------------------------------------------------------
#TASK 5.1 - Perform Data Quality Checks
#------------------------------------------------------------------------------------------------------------
#Perform data quality checks using custom routines or libraries such as Great Expectations


#------------------------------------------------------------------------------------------------------------
#TASK 5.2 - Populate database table
#------------------------------------------------------------------------------------------------------------
def _get_postgres_session(conn_id):
    postgres_hook = PostgresHook(postgres_conn_id=AIRFLOW_CONN_ID_DB_CURRENCY)
    session = postgresdb.create_session(postgres_hook.get_sqlalchemy_engine())
    return session

def _populate_source_db_tables(ti):
    msg   = ti.xcom_pull(task_ids=["perform_data_transformation"])[0]

    #A dict object is expected to be passed from the previous task
    if not isinstance(msg,dict):
        raise TypeError("A dict object was expected to be received from the preceeding task")

    #Retrieve the inter-tasks messages from XCOM
    df_curr_table = msg["df_curr_table"]
    json_response = msg["json_response"]
    
    #Populate both source_data_EUR and source_data_USD within the same transaction
    with _get_postgres_session(AIRFLOW_CONN_ID_DB_CURRENCY) as session:
        postgresdb.begin_session(session)

        try:
            upload_dt = datetime.datetime.now()
            df_curr_table['upload_dt'] = upload_dt #ensure same dt

            session.bulk_insert_mappings(source_data_EUR, df_curr_table.to_dict(orient="records"))
            session.add(source_data_USD(api_data=json_response,upload_dt=upload_dt))

        except:
            postgresdb.rollback_session(session)
            raise

        else:
            postgresdb.commit_session(session)
    



task_populate_source_euro_db_table = PythonOperator(
            task_id         = "populate_source_db_tables",
            dag             = currency_exchange_rates_dag,  
            python_callable = _populate_source_db_tables
    )
task_populate_source_euro_db_table.log_response   = log_response

#Define order of execution
task_check_endpoint_availavility >> task_http_get_currency_exchange_rates >> task_response_data_processor_cleansing >> task_response_data_processor_transformer >> task_populate_source_euro_db_table







  
  


