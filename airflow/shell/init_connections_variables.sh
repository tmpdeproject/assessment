#!/bin/bash

#Date: 23/10/2023
#Version: 1.0
#Author: Mark Grech
#Purpose: Creation and deletion of airflow connections. The script creates REST API endpoint definitions pertaining to the
#         data engineering project namely api_endpoint_openholidaysapi_org and api_endpoint_openerapi_com
#Ref: https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html


#Creation of an airflow connection to openholidaysapi.org REST API endpoint
conn_id="api_endpoint_openholidaysapi_org"
conn_host="https://openholidaysapi.org"
conn_type="http"
conn_description="A public REST API interface aimed at interrogating public/school holidays for various countries. Refer to: https://www.openholidaysapi.org/en/"
conn_json="{}"


./airflow.sh connections delete $conn_id 
./airflow.sh connections add $conn_id  \
  --conn-host $conn_host \
  --conn-description "$conn_description" \
  --conn-type $conn_type \
  
#Creation of an airflow connection to open.er-api.com REST API endpoint
conn_id="api_endpoint_openerapi_com_v6"
conn_host="https://open.er-api.com/v6/"
conn_description="A public REST API interface aimed at interrogating the latest currency rates. Refer to: https://www.exchangerate-api.com/docs/free"
conn_type="http"

./airflow.sh connections delete $conn_id 
./airflow.sh connections add $conn_id  \
  --conn-host $conn_host \
  --conn-description "$conn_description" \
  --conn-type $conn_type \


 #Creation of an database connection to docker postgress database container.
 #The database name can be passed/altered as it is passed as a parameter to the PostgreSQL operator 
conn_id="postgres_db_currency_exchange"
conn_uri="postgres://airflow_app_usr:@postgres:5432/db_currency_exchange"
conn_description="Database connection to postgreSQL dB db_currency_exchange database using application user"

./airflow.sh connections delete $conn_id 
./airflow.sh connections add $conn_id  \
  --conn-uri   $conn_uri \
  --conn-description "$conn_description" \

#Create variables
#Denotes the type of airflow setup - development/testing/staging/production.
./airflow.sh variables set environment development