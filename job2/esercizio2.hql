DROP TABLE IF EXISTS historical_stock;
DROP TABLE IF EXISTS historical_stock_prices;
DROP TABLE IF EXISTS historical_stock_join;
DROP TABLE IF EXISTS min_date_action;
DROP TABLE IF EXISTS max_date_action;
DROP TABLE IF EXISTS max_dateAndclose_for_sector_and_year;
DROP TABLE IF EXISTS max_volume;

CREATE TABLE historical_stock_prices (ticker_id STRING, open_act FLOAT, close_act FLOAT, adj_close FLOAT, lowThe FLOAT,highThe FLOAT,volume FLOAT,date_act DATE) 
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/piervy/Scrivania/historical_stock_prices_10000.csv'
                                      OVERWRITE INTO TABLE historical_stock_prices;

CREATE TABLE historical_stock (ticker_id STRING, exchange1 STRING, name STRING, sector STRING,industry STRING) 
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/piervy/Scrivania/historical_stocks.csv'
                                      OVERWRITE INTO TABLE historical_stock;

CREATE TABLE historical_stock_join AS 
             SELECT historical_stock_prices.ticker_id AS ticker_id, 
                    historical_stock_prices.close_act AS close_act,
                    historical_stock_prices.volume AS volume,
                    historical_stock_prices.date_act AS date_act,
                    historical_stock.sector AS sector 

             FROM historical_stock, historical_stock_prices
             WHERE historical_stock_prices.ticker_id = historical_stock.ticker_id AND historical_stock.sector != "N/A" AND historical_stock_prices.date_act >= '2009-01-01' AND historical_stock_prices.date_act <= '2018-12-31';



CREATE TABLE min_date_action AS
       SELECT ticker_id, sector, MIN(date_act) AS first_date
       FROM historical_stock_join
       GROUP BY ticker_id, sector;

SELECT * FROM min_date_action; 


CREATE TABLE max_date_action AS
       SELECT ticker_id, sector, MAX(date_act) AS last_date
       FROM historical_stock_join
       GROUP BY ticker_id, sector;


CREATE TABLE max_volume AS
             SELECT sector, year(date_act), ticker_id , MAX(volume) AS volume
             FROM historical_stock_join
             GROUP BY sector,year(date_act), ticker_id;

SELECT * FROM max_volume;

SELECT * FROM max_date_action;


DROP TABLE IF EXISTS max_volume;
