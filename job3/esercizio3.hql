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

CREATE TABLE historical_stock_prices_3Job (ticker_id STRING, open_act FLOAT, close_act FLOAT, adj_close FLOAT, lowThe FLOAT,highThe FLOAT,volume FLOAT,date_act DATE) 
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
        TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/insert/your/path/file.csv'
                                      OVERWRITE INTO TABLE historical_stock_prices_3Job;

CREATE TABLE historical_stock_3Job (ticker_id STRING, exchange1 STRING, name STRING, sector STRING,industry STRING) 
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
        TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/insert/your/path/file.csv'
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



/* Create TABLE for date min for any action*/
CREATE TABLE mindate_action AS
       SELECT ticker_id, month(date_act), MIN(date_act) AS first_date
       FROM historical_stock_join_2017
       GROUP BY ticker_id, month(date_act);



CREATE TABLE maxdate_action AS
       SELECT ticker_id, month(date_act), MAX(date_act) AS last_date
       FROM historical_stock_join_2017
       GROUP BY ticker_id, month(date_act);



/* Create TABLE with date min and close for any action*/
CREATE TABLE first_close_action_2017 AS
             SELECT t2.ticker_id, t2.first_date , t1.close_act, t1.name
             FROM historical_stock_join_2017 AS t1,
                  mindate_action AS t2
             WHERE t1.ticker_id = t2.ticker_id AND t1.date_act = t2.first_date;



CREATE TABLE last_close_action_2017 AS
             SELECT t2.ticker_id, t2.last_date , t1.close_act
             FROM historical_stock_join_2017 AS t1,
                  maxdate_action AS t2
             WHERE t1.ticker_id = t2.ticker_id AND t1.date_act = t2.last_date;


/* Create TABLE for compute percentage variation for every action*/
CREATE TABLE var_percent_for_ticker AS
             SELECT t2.ticker_id AS ticker_id,
                    month(t2.first_date) AS first_date,
                    t2.name,
                    ((t3.close_act-t2.close_act)/t2.close_act)*100 AS var_percent
             FROM first_close_action_2017 AS t2,
                  last_close_action_2017 AS t3
            WHERE t2.ticker_id = t3.ticker_id
              AND month(t2.first_date) = month(t3.last_date);                   
              


/* Create TABLE for compute difference for any var_percent for next table */
CREATE TABLE var_percent_action_with_treshold AS
             SELECT t2.ticker_id AS ticker_id,
                    t1.ticker_id AS ticker_id2,
                    t1.name AS name1,
                    t2.name AS name2,
                    t2.first_date AS first_date,
                    t1.var_percent AS var_percent1,
                    t2.var_percent AS var_percent2,
                    ABS(t1.var_percent - t2.var_percent) AS var_percent_diff
             FROM var_percent_for_ticker AS t1,
                  var_percent_for_ticker AS t2  
             WHERE t1.ticker_id <> t2.ticker_id
               AND t2.first_date = t1.first_date;


CREATE TABLE output_job3 AS
             SELECT t1.first_date AS month_date,
                    t1.ticker_id,
                    t1.name1,
                    t1.var_percent1,
                    t1.ticker_id2,
                    t1.name2,
                    t1.var_percent2
             FROM var_percent_action_with_treshold AS t1
             WHERE t1.var_percent_diff <=1
             ORDER BY month_date;


SELECT * FROM output_job3
LIMIT 10;


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








