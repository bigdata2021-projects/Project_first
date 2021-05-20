DROP TABLE IF EXISTS historical_stock;
DROP TABLE IF EXISTS historical_stock_prices;
DROP TABLE IF EXISTS historical_stock_join;
DROP TABLE IF EXISTS min_date_action;
DROP TABLE IF EXISTS max_date_action;
DROP TABLE IF EXISTS max_dateAndclose_for_sector_and_year;
DROP TABLE IF EXISTS min_dateAndclose_for_sector_and_year;
DROP TABLE IF EXISTS sum_min_sector;
DROP TABLE IF EXISTS sum_max_sector;
DROP TABLE IF EXISTS action_first_close_for_year_and_sector;
DROP TABLE IF EXISTS action_last_close_for_year_and_sector;
DROP TABLE IF EXISTS var_percent_act_for_year_and_sector;
DROP TABLE IF EXISTS max_var_percent_act_for_year_and_sector;
DROP TABLE IF EXISTS sum_volume;
DROP TABLE IF EXISTS max_volume_sector_year;
DROP TABLE IF EXISTS final_sector_historical_stocks;


CREATE TABLE historical_stock_prices (ticker_id STRING, open_act FLOAT, close_act FLOAT, adj_close FLOAT, lowThe FLOAT,highThe FLOAT,volume INT,date_act DATE) 
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
	TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/francesco/Desktop/Progetto1/dataset/historical_stock_prices_10000.csv'
                                      OVERWRITE INTO TABLE historical_stock_prices;

CREATE TABLE historical_stock (ticker_id STRING, exchange1 STRING, name STRING, sector STRING,industry STRING) 
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
	TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/francesco/Desktop/Progetto1/dataset/historical_stocks.csv'
                                      OVERWRITE INTO TABLE historical_stock;
/* JOIN BETWEEN TABLE */
CREATE TABLE historical_stock_join AS 
             SELECT historical_stock_prices.ticker_id AS ticker_id, 
                    historical_stock_prices.close_act AS close_act,
                    historical_stock_prices.volume AS volume,
                    historical_stock_prices.date_act AS date_act,
                    historical_stock.sector AS sector 

             FROM historical_stock, historical_stock_prices
             WHERE historical_stock_prices.ticker_id = historical_stock.ticker_id AND historical_stock.sector != "N/A" AND historical_stock_prices.date_act >= '2009-01-01' AND historical_stock_prices.date_act <= '2018-12-31';


/* Job A */
CREATE TABLE min_date_action AS
       SELECT ticker_id, sector, year(date_act),MIN(date_act) AS first_date
       FROM historical_stock_join
       GROUP BY ticker_id, sector,year(date_act);

SELECT * FROM min_date_action; 


CREATE TABLE max_date_action AS
       SELECT ticker_id, sector, year(date_act),MAX(date_act) AS last_date
       FROM historical_stock_join
       GROUP BY ticker_id, sector,year(date_act);

SELECT * FROM max_date_action;


CREATE TABLE max_dateAndclose_for_sector_and_year AS 
             SELECT historical_stock_join.sector AS sector,
                    historical_stock_join.date_act AS last_date,
                    historical_stock_join.ticker_id AS ticker_id,
                    historical_stock_join.close_act AS close_act
             FROM historical_stock_join,max_date_action
             WHERE historical_stock_join.ticker_id = max_date_action.ticker_id 
             AND historical_stock_join.sector = max_date_action.sector
             AND historical_stock_join.date_act = max_date_action.last_date;

SELECT * FROM max_dateAndclose_for_sector_and_year;

CREATE TABLE min_dateAndclose_for_sector_and_year AS 
             SELECT historical_stock_join.sector AS sector,
                    historical_stock_join.date_act AS first_date,
                    historical_stock_join.ticker_id AS ticker_id,
                    historical_stock_join.close_act AS close_act
             FROM historical_stock_join,min_date_action
             WHERE historical_stock_join.ticker_id = min_date_action.ticker_id 
             AND historical_stock_join.sector = min_date_action.sector
             AND historical_stock_join.date_act= min_date_action.first_date;

SELECT * FROM  min_dateAndclose_for_sector_and_year;


CREATE TABLE sum_min_sector AS  
             SELECT sector, year(first_date) AS year_sum,
                    SUM(close_act) as first_close_act_sum_sector
             FROM min_dateAndclose_for_sector_and_year
             GROUP BY sector , year(first_date);

CREATE TABLE sum_max_sector AS
             SELECT sector, year(last_date) AS year_sum,
                    SUM(close_act) as last_close_act_sum_sector
             FROM max_dateAndclose_for_sector_and_year
             GROUP BY sector , year(last_date);


/* JOB B */
CREATE TABLE action_first_close_for_year_and_sector AS
             SELECT sector, year(first_date) AS first_date, ticker_id, close_act AS first_close
             FROM min_dateAndclose_for_sector_and_year;

SELECT * FROM action_first_close_for_year_and_sector;           

CREATE TABLE action_last_close_for_year_and_sector AS
             SELECT sector, year(last_date) AS last_date, ticker_id, close_act AS last_close
             FROM max_dateAndclose_for_sector_and_year;

SELECT * FROM action_last_close_for_year_and_sector;

CREATE TABLE var_percent_act_for_year_and_sector AS
              SELECT t1.sector,
                     t1.ticker_id,
                     t1.first_date,
                     ((t2.last_close - t1.first_close)/t1.first_close)*100 AS act_var_percent
              FROM action_first_close_for_year_and_sector AS t1, action_last_close_for_year_and_sector AS t2
              WHERE t1.sector= t2.sector 
              AND t1.ticker_id = t2.ticker_id 
              AND t1.first_date = t2.last_date;

SELECT * FROM var_percent_act_for_year_and_sector;

CREATE TABLE max_var_percent_act_for_year_and_sector AS
             SELECT t1.sector AS sector,
                    t1.first_date AS first_date,
                    t1.ticker_id,
                    t2.max_var_percent AS max_var_percent
             FROM var_percent_act_for_year_and_sector AS t1 ,( SELECT st1.sector AS sector,
                                                                      st1.first_date, 
                                                                      MAX(st1.act_var_percent) AS max_var_percent
                                                                      FROM var_percent_act_for_year_and_sector AS st1                  
                                                                      GROUP BY st1.sector, st1.first_date) AS t2
             WHERE t1.sector = t2.sector 
             AND t1.first_date = t2.first_date
             AND t1.act_var_percent = t2.max_var_percent;
            

SELECT * FROM max_var_percent_act_for_year_and_sector;

/* JOB C */


CREATE TABLE sum_volume AS
             SELECT sector, year(date_act) as date_act, ticker_id , sum(volume) AS sum_volume
             FROM historical_stock_join
             GROUP BY sector,year(date_act), ticker_id;

SELECT * FROM sum_volume;

CREATE TABLE max_volume_sector_year AS
             SELECT t1.sector AS sector, 
                    t1.date_act AS date_act,
                    t1.ticker_id AS ticker_id,
                    t2.max_sum_volume AS max_sum_volume
             FROM sum_volume as t1, ( SELECT st1.sector AS sector, 
                                           st1.date_act AS date_act,
                                             MAX(st1.sum_volume) AS max_sum_volume
                                             FROM sum_volume AS st1                  
                                             GROUP BY st1.sector, st1.date_act) AS t2
             WHERE t1.sector = t2.sector 
               AND t1.date_act = t2.date_act
               AND t1.sum_volume = t2.max_sum_volume;


/* FINAL OUTPUT */
CREATE TABLE final_sector_historical_stocks AS
              SELECT t1.sector AS sector,
                     t1.first_date AS first_date,
                     ((t3.last_close_act_sum_sector-t2.first_close_act_sum_sector)/t2.first_close_act_sum_sector)*100 AS sector_var_percent,
                     t1.ticker_id AS ticker_id,
                     t1.max_var_percent AS max_var_percent,
                     t4.max_sum_volume AS max_sum_volume
              FROM max_var_percent_act_for_year_and_sector AS t1,
                   sum_min_sector AS t2,
                   sum_max_sector AS t3,
                   max_volume_sector_year AS t4
              WHERE t1.sector = t2.sector
              AND   t2.sector = t3.sector
              AND   t3.sector = t4.sector             
              AND   t2.year_sum = t1.first_date 
              AND   t2.year_sum = t3.year_sum
              AND   t3.year_sum = t4.date_act
              ORDER BY sector;

SELECT * FROM final_sector_historical_stocks;






