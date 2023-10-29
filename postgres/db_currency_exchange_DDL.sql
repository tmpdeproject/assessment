--Purpose: DDL script to create the database, application user and the database tables
--         Grant miminal access rights to the application user
--Version 3.0
--Date: 28/10/2023
--Database: PostgreSQL 

--DATABASE CREATION
DROP DATABASE db_currency_exchange;

CREATE DATABASE db_currency_exchange OWNER AIRFLOW ENCODING='UTF8';  


--USER CREATION
DO
$do$
BEGIN

  IF  EXISTS ( SELECT 1 FROM pg_catalog.pg_user
               WHERE usename='airflow_app_usr' 
             ) THEN
    
   RAISE NOTICE E'APPLICATION USER ALREADY EXISTS. DROPPING USER FIRST\n';  
   DROP OWNED BY airflow_app_usr;
   DROP USER airflow_app_usr;     
  END IF;

  RAISE NOTICE E'CREATING NEW APPLICATION USER\n';  
  CREATE USER airflow_app_usr WITH ENCRYPTED PASSWORD 'AYgAQyBggBEEU';
  GRANT CONNECT ON DATABASE db_currency_exchange TO airflow_app_usr;
  GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO airflow_app_usr; 
  GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO airflow_app_usr;
  GRANT USAGE ON SCHEMA public TO airflow_app_usr;

END
$do$;


--TABLE CREATION
DROP TABLE IF EXISTS "source_data_USD";
CREATE TABLE "source_data_USD" (
  ID SERIAL PRIMARY KEY,
  upload_dt timestamp,
  api_data json
);

DROP TABLE IF EXISTS "source_data_EUR";
CREATE TABLE "source_data_EUR" (
  ID SERIAL PRIMARY KEY,
  upload_dt timestamp,
  currency varchar(3),
  rate numeric(10,4)
);

DROP TABLE IF EXISTS be_holidays;
CREATE TABLE be_holidays (
  ID varchar PRIMARY KEY,
  event_date date,
  holiday_description varchar
);


--Creation of VIEW exchange_rates
DROP VIEW IF EXISTS exchange_rates;
CREATE VIEW exchange_rates
AS
WITH cte_base AS
(
    SELECT "source_data_EUR".id,
            upload_dt,

            --table is designated to hold euro base currency rates.  
            (SELECT currency 
             FROM "source_data_EUR" as source_data_EUR_inner  
             WHERE source_data_EUR_inner.upload_dt = "source_data_EUR".upload_dt and rate=1) as base,

        currency,rate, 
        "holiday_description" as is_holiday
        
    FROM "source_data_EUR" LEFT JOIN "be_holidays" ON date(upload_dt) = "be_holidays".event_date 
),
cte_collection_date
AS
(
 SELECT id,
        upload_dt,
        to_timestamp(api_data->>'time_last_update_utc','DY, DD MON YYYY HH24:MI:SS TZH') as collection_dt
 FROM "public"."source_data_USD"
)
SELECT cte_collection_date.collection_dt, cte_base.base, cte_base.currency, cte_base.rate,cte_base.is_holiday
FROM cte_base LEFT JOIN cte_collection_date ON cte_base.upload_dt = cte_collection_date.upload_dt

