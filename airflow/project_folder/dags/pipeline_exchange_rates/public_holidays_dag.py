import json
import pathlib
import datetime 
import numpy  as np
import pandas as pd

from   datetime import datetime, timedelta,date
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

#API/HTTP
from requests.compat import urljoin

#Custom imports
from   includes.db_models.db_currency_exchange import be_holidays
import includes.db_postgres.postgres as postgresdb 

#Constant defintions
AIRFLOW_CONN_ID_DB_CURRENCY  = "postgres_db_currency_exchange"   
AIRFLOW_CONN_ID              = "api_endpoint_openholidaysapi_org" #Connection ID as specified with airflow engine. Go to admin | connections. 

DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    #'depends_on_past': False,
    #'email_on_failure': False,
    #'email_on_retry': False,
    'retries': 1, 
    'retry_delay': timedelta(minutes=10)
}

#Read airflow variable to establish whether we should by default log responses to log file
log_response = True if Variable.get("environment",True) != 'production' else False


#------------------------------------------------------------------------------------------------------------
# HELPER FUNCIONS
#------------------------------------------------------------------------------------------------------------

def _verify_api_response_by_http_status_code(response):
    if response and response.status_code == 200:
        return True
    else:
        return False

#TO DO. The entire client REST API functionality can be encapsulated in a class.
def _get_service_endpoint(name):
    
    SERVICE_ENDPOINTS = {'PublicHolidays':'PublicHolidays',
                        'PublicHolidaysByDate':'PublicHolidaysByDate',
                        'SchoolHolidays':'SchoolHolidays',
                        'SchoolHolidays':'SchoolHolidaysByDate',
                        }

    return SERVICE_ENDPOINTS.get(name)

def _holidays_publicholidays_endpoint_params(country_iso_code:str, valid_from:date,valid_to:date,language_iso_code:str=None, subdivision_code:str=None):
    '''
    Builds the parameters for the Holdidays/PublicHoldays endpoint.
    
    country_iso_code:   ISO 3166-1 code of the country
    valid_from:         Start of the date range
    valid_to:           End of the date range
    language_iso_code:  ISO-639-1 code of a language or empty
    subdivision_code:   Code of the subdivision or empty eg. DE-BE

    REF: https://openholidaysapi.org/swagger/index.html
    '''
    data    = dict()

    #Validate parameters
    if not country_iso_code:
        raise ValueError('Parameter valid_from must be specified')

    if not valid_from:
        raise ValueError('Parameter valid_from must be specified')

    if not valid_to:
        raise ValueError('Parameter valid_to must be specified')

    if not (isinstance(country_iso_code,str) and len(country_iso_code) == 2):
        raise ValueError('Invalid value specified for country_iso_code: {}'.format(country_iso_code))
    
    if language_iso_code and not (isinstance(language_iso_code,str) and len(language_iso_code) == 2):
        raise ValueError('Invalid value specified for language_iso_code: {}'.format(language_iso_code))
   
    #Add to dict    
    data['validFrom']      = valid_from.strftime("%Y-%m-%d")
    data['validTo']        = valid_to.strftime("%Y-%m-%d")
    data['countryIsoCode'] = country_iso_code.upper()

    if language_iso_code:
        data['languageIsoCode'] = language_iso_code.upper()

    if subdivision_code:
        data['subdivisionCode'] = subdivision_code.upper()

    return data


#------------------------------------------------------------------------------------------------------------
#DEFINITION OF DAG
#------------------------------------------------------------------------------------------------------------
bank_holidays_dag = DAG(
    dag_id            = "retrieve_bank_holidays_dag",
    description       = "Downloads bank holidays for a particular country pertaining for the specified period of time ",
    start_date        = datetime(2022,12,1), #TO DO verify start date of the DAG
    schedule          = "@daily",            #TO DO verify schedule interval of the DAG
    catchup           = False,
    default_args      = DEFAULT_DAG_ARGS
)

#------------------------------------------------------------------------------------------------------------
#TASK 1 - VERIFY WHETHER ENDPOINT IS AVAILABLE
#------------------------------------------------------------------------------------------------------------
#TO DO: Paramerize outside DAG script
countryIsoCode  = "BE"
languageIsoCode = "EN"
validFrom       = datetime(2023,1,1)  
validTo         = datetime(2023,1,1)  #No need to get an entire year worth of data when checking endpoint's health...

task_check_endpoint_availability = HttpSensor(
            task_id        = 'verify_endpoint_availablity',
            endpoint       = '',
            http_conn_id   = AIRFLOW_CONN_ID,
            dag            = bank_holidays_dag,
            request_params = {},
            poke_interval  = 5 #When using poke mode, this is the time in seconds that the sensor waits before checking the condition again.
        )
task_check_endpoint_availability.log_response   = log_response
task_check_endpoint_availability.request_params = _holidays_publicholidays_endpoint_params(countryIsoCode,validFrom,validTo,languageIsoCode)
task_check_endpoint_availability.endpoint       = _get_service_endpoint('PublicHolidays')


#------------------------------------------------------------------------------------------------------------
#TASK 2 - RETRIEVAL OF BANK HOLIDAYS VIA HTTP ENDPOINT
#------------------------------------------------------------------------------------------------------------
#TO DO: Paramerize outside DAG script
countryIsoCode  = "BE"
languageIsoCode = "EN"
validFrom       = datetime(datetime.now().year,1,1)    #"2023-01-01"
validTo         = datetime(datetime.now().year,12,31)  #"2023-12-31"


#Retrieve bank holidays data
#example url: https://openholidaysapi.org/PublicHolidays?countryIsoCode=BE&languageIsoCode=EN&validFrom=2023-01-01&validTo=2023-12-31
#endpoint= 'PublicHolidays', #'countryIsoCode=BE&languageIsoCode=EN&validFrom=2023-01-01&validTo=2023-12-31',
task_http_get_bank_holidays = SimpleHttpOperator(
            task_id   ='retrieve_bank_holidays',
            method    ='GET',
            http_conn_id=AIRFLOW_CONN_ID,
            headers={"Content-Type": "application/json"},
            dag       =bank_holidays_dag,
    )

task_http_get_bank_holidays.log_response   = log_response
task_http_get_bank_holidays.data           = _holidays_publicholidays_endpoint_params(countryIsoCode,validFrom,validTo,languageIsoCode)
task_http_get_bank_holidays.endpoint       = _get_service_endpoint('PublicHolidays')
task_http_get_bank_holidays.response_check = lambda response : _verify_api_response_by_http_status_code(response)


#------------------------------------------------------------------------------------------------------------
#TASK 3 - DATA CLEANSING
#------------------------------------------------------------------------------------------------------------
def _data_processor_cleansing(ti):
    '''
    Stub function that performs data cleansing prior to the application of data transformations
    '''
    #retrieve msgs sent between tasks via XCOM.
    json_response   = ti.xcom_pull(task_ids=["retrieve_bank_holidays"]) 

    #cConvert to df    
    df_holidays = pd.DataFrame(json.loads(json_response[0]))

    #convert dt fields
    df_holidays['startDate'] = pd.to_datetime(df_holidays['startDate'],format='%Y-%m-%d')
    df_holidays['endDate']   = pd.to_datetime(df_holidays['endDate'],  format='%Y-%m-%d')

    #remove unwanted columns
    #do not drop type and nationwide columns. Will be needed in upstream tasks
    #df_holidays.drop(columns=['id'],inplace=True)
    
    return {'df_holidays':df_holidays} 


task_response_data_processor_cleansing = PythonOperator(
            task_id         ='perform_data_cleansing',
            dag             = bank_holidays_dag,  
            python_callable = _data_processor_cleansing
    )
task_response_data_processor_cleansing.log_response   = log_response

#------------------------------------------------------------------------------------------------------------
#TASK 4 - DATA TRANSFORMATION
#------------------------------------------------------------------------------------------------------------
def _data_processor_transformation(ti):
    '''
    Performs any data processing on the data received from the API endpoint. 
    '''
    def generate_description_text(row):
        nationwide  = 'nationwide' if row['nationwide'] == True else ''
        public      = row['type'].lower()
        description = "{text} ({nationwide} {public})".format(text=row['text'],nationwide=nationwide,public=public)
    
        return description

    #main
   
    #retrieve msgs sent between tasks via XCOM.
    dict_msg = ti.xcom_pull(task_ids=["perform_data_cleansing"])[0] 
    
    #get the dataframe
    df_holidays = dict_msg['df_holidays']

    #explode name to separate ds columns
    ds = df_holidays['name'].explode()
    ds = ds.apply(pd.Series)
    df_holidays = pd.concat([df_holidays,ds],axis=1)

    #generate_description_text column. We could have used the given text column.
    #Added data manipulation function
    df_holidays['holiday_description'] = df_holidays.apply(generate_description_text,axis=1)

    #Compute diff between the two dates
    df_holidays['datediff'] = (df_holidays['endDate'] - df_holidays['startDate']).dt.days

    #Check for multi-day holidays
    df_multi_day_holidays = df_holidays[(df_holidays['datediff'] > 0)]
    if df_multi_day_holidays.shape[0] > 0:
        logging.debug('Multiday holidays were found in dataset: {}'.format(df_multi_day_holidays.shape[0]))

    #Compute date range
    df_holidays['daterange'] = df_holidays.apply(lambda row: pd.date_range(row['startDate'],row['endDate']),axis=1)
    df_holidays = df_holidays.explode('daterange')

    #Clean and rename dataframe
    df_holidays.drop(columns=['startDate','endDate','datediff','type','name','nationwide','datediff','language','text'],inplace=True)
    df_holidays.rename(columns={'daterange':'event_date'},inplace=True)

    return {'df_holidays':df_holidays} 


task_response_data_processor_transformer = PythonOperator(
            task_id ='perform_data_transformation',
            dag     = bank_holidays_dag,  
            python_callable = _data_processor_transformation
    )
task_response_data_processor_transformer.log_response  = log_response

#------------------------------------------------------------------------------------------------------------
#TASK 5.1 - Check for existing rows in DB prior to populaton
#------------------------------------------------------------------------------------------------------------
#In this contrived assessment the be_holdays table stores a few rows.
#In real-life transactional tables store large volumes of data.
#It would be best to build a query and check for any matching rows using IDs at the DB-side.
#If the table is managable in volumne it is possible to perform row matching on the client-end ie. pandas df

#The task will failed if executed repeatedly due to duplicate primary keys (duplicate key value violates unique constraints) 

#------------------------------------------------------------------------------------------------------------
#TASK 5.2 - Data Quality Checks
#------------------------------------------------------------------------------------------------------------
#Perform data quality checks using custom routines or libraries such as Great Expectations


#------------------------------------------------------------------------------------------------------------
#TASK 5.3 - Populate database table - be_holidays
#------------------------------------------------------------------------------------------------------------
def _get_postgres_session(conn_id):
    postgres_hook = PostgresHook(postgres_conn_id=AIRFLOW_CONN_ID_DB_CURRENCY)
    session = postgresdb.create_session(postgres_hook.get_sqlalchemy_engine())
    return session

def _populate_be_holidays_db_table(ti):
    msg   = ti.xcom_pull(task_ids=["perform_data_transformation"])[0]

    #A dict object is expected to be passed from the previous task
    if not isinstance(msg,dict):
        raise TypeError("A dict object was expected to be received from the preceeding task")

    #Retrieve the inter-tasks messages from XCOM
    df_holidays = msg["df_holidays"]

    #Populate both source_data_EUR and source_data_USD within the same transaction
    with _get_postgres_session(AIRFLOW_CONN_ID_DB_CURRENCY) as session:
        postgresdb.begin_session(session)

        try:
            session.bulk_insert_mappings(be_holidays, df_holidays.to_dict(orient="records"))
          
        except:
            postgresdb.rollback_session(session)
            raise

        else:
            postgresdb.commit_session(session)
            
    return df_holidays


task_populate_be_holidays_db_table = PythonOperator(
            task_id         = "populate_be_holidays_table",
            dag             = bank_holidays_dag,  
            python_callable = _populate_be_holidays_db_table
    )
task_populate_be_holidays_db_table.log_response   = log_response


#Define order of execution
task_check_endpoint_availability >> task_http_get_bank_holidays >> task_response_data_processor_cleansing >> task_response_data_processor_transformer >> task_populate_be_holidays_db_table



 

  
  


