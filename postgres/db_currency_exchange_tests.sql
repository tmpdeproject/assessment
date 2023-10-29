--Purpose:   Basic tests that were performed after view creation. Relates to the population of tables after the execution of Airfow data pipelines.
--Pipelines: retrieve_bank_holidays_dag, retrieve_currency_exchange_rates_dag 
--Date:      28/10/2023
--Version:   1.0
--Datebase:  Postgres
--Schema:    db_currency_exchange 


--Simple check that the confirms that the base currency is EUR 
--Should return EUR/EUR 1
SELECT *
FROM exchange_rates2
WHERE rate = 1

--Simple check that the number of times the data pipeline was executed
--The data pipeline was executed twice after the tables were truncated
SELECT collection_dt,count(distinct base)
FROM exchange_rates
GROUP BY collection_dt

SELECT upload_dt,count(id)
FROM "source_data_USD" 
GROUP BY upload_dt

DROP VIEW IF EXISTS exchange_rates;
