DROP TABLE IF EXISTS action_temp6;
DROP TABLE IF EXISTS temp2;
DROP TABLE IF EXISTS action;
DROP TABLE IF EXISTS max_date_action;
DROP TABLE IF EXISTS min_date_action;
DROP TABLE IF EXISTS first_close_action;
DROP TABLE IF EXISTS last_close_action;
DROP TABLE IF EXISTS min_action;
DROP TABLE IF EXISTS max_action;
DROP TABLE IF EXISTS final;
DROP TABLE IF EXISTS final1;


CREATE TABLE action_temp6 (ticker_id STRING, open_act FLOAT, close_act FLOAT, adj_close FLOAT, lowThe FLOAT,highThe FLOAT,volume FLOAT,date_act DATE) 
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/francesco/Desktop/prova_bigdata.csv'
                                      OVERWRITE INTO TABLE action_temp6;

CREATE TABLE max_date_action AS
       SELECT ticker_id, MAX(date_act) AS last_date
       FROM action_temp6
       GROUP BY ticker_id;

SELECT * FROM max_date_action
LIMIT 10;

CREATE TABLE min_date_action AS
       SELECT ticker_id, MIN(date_act) AS first_date
       FROM action_temp6
       GROUP BY ticker_id;

SELECT * FROM min_date_action
LIMIT 10;

CREATE TABLE first_close_action AS
       SELECT action_temp6.ticker_id AS ticker_id_first_close, action_temp6.date_act AS first_date, action_temp6.close_act AS close_act
       FROM action_temp6,min_date_action 
       WHERE action_temp6.ticker_id = min_date_action.ticker_id AND action_temp6.date_act=min_date_action.first_date;

SELECT * FROM first_close_action
LIMIT 10;

CREATE TABLE last_close_action AS
       SELECT action_temp6.ticker_id AS ticker_id_last_close, action_temp6.date_act AS last_date, action_temp6.close_act AS close_act
       FROM action_temp6,max_date_action 
       WHERE action_temp6.ticker_id = max_date_action.ticker_id AND action_temp6.date_act=max_date_action.last_date;
       
SELECT * FROM last_close_action
LIMIT 10;

CREATE TABLE min_action AS
       SELECT ticker_id AS ticker_id_min_action, MIN(lowThe) AS low_act
       FROM action_temp6
       GROUP BY ticker_id;

SELECT * FROM min_action
LIMIT 10;

CREATE TABLE max_action AS
       SELECT ticker_id AS ticker_id_max_action, MAX(highThe) AS high_act
       FROM action_temp6
       GROUP BY ticker_id;


SELECT * FROM max_action
LIMIT 10;


       
SELECT min_action.ticker_id_min_action AS ticker_id,
         first_close_action.first_date AS first_date,
        last_close_action.last_date AS last_date,
        max_action.high_act AS high_act,
        min_action.low_act AS low_act,
        first_close_action.close_act AS first_close,
        last_close_action.close_act AS last_close,
        ((first_close_action.close_act - last_close_action.close_act)/first_close_action.close_act)*100 AS var_percent

       FROM first_close_action,last_close_action,min_action,max_action
       WHERE min_action.ticker_id_min_action = first_close_action.ticker_id_first_close
        AND first_close_action.ticker_id_first_close = last_close_action.ticker_id_last_close
        AND last_close_action.ticker_id_last_close = max_action.ticker_id_max_action
        AND max_action.ticker_id_max_action = min_action.ticker_id_min_action;
        

        








