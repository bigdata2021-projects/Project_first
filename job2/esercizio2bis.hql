DROP TABLE IF EXISTS min_date_for_sector_and_year;
DROP TABLE IF EXISTS min_dateAndclose_for_sector_and_year;

DROP TABLE IF EXISTS max_dateAndclose_for_sector_and_year;
DROP TABLE IF EXISTS var_sector;
DROP TABLE IF EXISTS max_volume;


SELECT * from historical_stock_join;


CREATE TABLE max_dateAndclose_for_sector_and_year AS 
             SELECT historical_stock_join.sector AS sector,
                    historical_stock_join.date_act AS last_date,
                    historical_stock_join.ticker_id AS ticker_id,
                    historical_stock_join.close_act AS close_act
             FROM historical_stock_join,max_date_action
             WHERE historical_stock_join.ticker_id = max_date_action.ticker_id 
             AND historical_stock_join.sector = max_date_action.sector
             AND historical_stock_join.date_act= max_date_action.last_date;

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

SELECT * FROM min_dateAndclose_for_sector_and_year; 

CREATE TABLE max_volume AS
             SELECT sector, year(date_act), ticker_id , MAX(volume) AS volume
             FROM historical_stock_join
             GROUP BY sector,year(date_act), ticker_id;

SELECT * FROM max_volume;

DROP TABLE IF EXISTS max_volume;
DROP TABLE IF EXISTS var_sector;

