--Purpose:   Basic tests that were performed after view creation. Relates to the population of tables after the execution of Airfow data pipelines.
--Pipelines: retrieve_bank_holidays_dag, retrieve_currency_exchange_rates_dag 
--Date:      28/10/2023
--Version:   1.0
--Datebase:  Postgres
--Schema:    db_currency_exchange 

--Test 1
--Simple check that the confirms that the base currency is EUR 
--Should return EUR/EUR 1
SELECT *
FROM exchange_rates2
WHERE rate = 1

--Test 2
--Simple check that the number of times the data pipeline was executed
--The data pipeline was executed twice after the tables were truncated
SELECT collection_dt,count(distinct base)
FROM exchange_rates
GROUP BY collection_dt
--2

--Test 3
--Insert a new record having event date set identical to source_data_EUR.upload_dt (29/10/2023)
--Expected outcome - holiday_description should read - Test holiday 2023-10-29
INSERT INTO be_holidays(id, event_date,holiday_description)
VALUES
('c41121ed-b6fb-c9a6-bc9b-574c82929e7e','2023-10-29','Test holiday 2023-10-29')

select  collection_dt,base,currency,rate,is_holiday
from public.exchange_rates
--2023-10-29T00:02:31+00:00	EUR	GIP	0.8709	    Test holiday 2023-10-29
--2023-10-29T00:02:31+00:00	EUR	GMD	68.3663	    Test holiday 2023-10-29
--2023-10-29T00:02:31+00:00	EUR	GNF	9072.7923	Test holiday 2023-10-29
--2023-10-29T00:02:31+00:00	EUR	GTQ	8.2769	    Test holiday 2023-10-29
--2023-10-29T00:02:31+00:00	EUR	GYD	220.8357	Test holiday 2023-10-29

