DROP TABLE IF EXISTS historical_stock_3Job;
DROP TABLE IF EXISTS historical_stock_prices_3Job;
DROP TABLE IF EXISTS historical_stock_join_2017;

CREATE TABLE historical_stock_prices_3Job (ticker_id STRING, open_act FLOAT, close_act FLOAT, adj_close FLOAT, lowThe FLOAT,highThe FLOAT,volume FLOAT,date_act DATE) 
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/piervy/Scrivania/historical_stock_prices_100000.csv'
                                      OVERWRITE INTO TABLE historical_stock_prices_3Job;

CREATE TABLE historical_stock_3Job (ticker_id STRING, exchange1 STRING, name STRING, sector STRING,industry STRING) 
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/piervy/Scrivania/historical_stocks.csv'
                                      OVERWRITE INTO TABLE historical_stock_3Job;


/* JOIN BETWEEN TABLE */
CREATE TABLE historical_stock_join_2017 AS 
             SELECT historical_stock_prices_3Job.ticker_id AS ticker_id, 
                    historical_stock_prices_3Job.close_act AS close_act,
                    historical_stock_prices_3Job.volume AS volume,
                    historical_stock_prices_3Job.date_act AS date_act,
                    historical_stock_3Job.name AS name

             FROM historical_stock_3job, historical_stock_prices_3Job
             WHERE historical_stock_prices_3Job.ticker_id = historical_stock_3Job.ticker_id AND historical_stock_3Job.sector != "N/A" AND historical_stock_prices_3Job.date_act >= '2017-01-01' AND historical_stock_prices_3Job.date_act <= '2017-12-31';

SELECT * FROM historical_stock_join_2017;



DROP TABLE IF EXISTS historical_stock_3Job;
DROP TABLE IF EXISTS historical_stock_prices_3Job;
DROP TABLE IF EXISTS historical_stock_join_2017;
DROP TABLE IF EXISTS mindate_action;
DROP TABLE IF EXISTS maxdate_action;
DROP TABLE IF EXISTS var_percent_for_ticker;
DROP TABLE IF EXISTS first_close_action_2017;
DROP TABLE IF EXISTS last_close_action_2017;
DROP TABLE IF EXISTS var_percent_for_ticker;
DROP TABLE IF EXISTS var_percent_action_with_treshold;
DROP TABLE IF EXISTS output_job3; 

