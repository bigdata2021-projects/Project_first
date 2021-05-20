#!/usr/bin/env python3
"""spark application"""
import argparse
from pyspark.sql import SparkSession
from datetime import datetime
from itertools import combinations

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path1", type=str, help="Input file path1")
parser.add_argument("--input_path2", type=str, help="Input file path1")
parser.add_argument("--output_path", type=str, help="Output folder path")
# parse arguments
args = parser.parse_args()
input_filepath1, input_filepath2, output_filepath = args.input_path1, args.input_path2, args.output_path
# initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Spark Job3") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

""""
Un job che sia in grado di generare un report contenente,
per ciascun settore e per ciascun anno del periodo 2009-2018: 
(a) la variazione percentuale della quotazione del settore1 nell’anno, 
(b) l’azione del settore che ha avuto il maggior incremento percentuale nell’anno (con indicazione dell’incremento) 
(c) l’azione del settore che ha avuto il maggior volume di transazioni nell’anno (con indicazione del volume). 
Il report deve essere ordinato per nome del settore.
"""

lines_RDD_1 = spark.sparkContext.textFile(input_filepath1)

lines_RDD_2 = spark.sparkContext.textFile(input_filepath2)

stock_prices_RDD = lines_RDD_1.map(f=lambda line: line.strip().split(",")) \
    .filter(f=lambda l: 2009 <= datetime.strptime(l[7], '%Y-%m-%d').year <= 2018) \
    .map(f=lambda pair: (pair[0], pair))

# (ticker ,(name,sector))
stock_RDD = lines_RDD_2.map(f=lambda line: line.strip().split(",")) \
    .map(f=lambda pair: (pair[0], (pair[2], pair[3])))


def build_RDD(el):
    ticker = el[0]
    close_act = float(el[1][0][2])
    volume = float(el[1][0][6])
    date = datetime.strptime(el[1][0][7], '%Y-%m-%d')
    year = date.year
    name = el[1][1][0]
    sector = el[1][1][1]

    return ticker, name, year, date, sector, close_act, volume


# (ticker , ((tuplaDa7Elementi) , (name,sector))
# (ticker, name ,year, date, sector , close_act, volume)
stock_join = stock_prices_RDD.join(stock_RDD).map(f=build_RDD)

# PUNTO C
# (sector, year , ticker), volume)
sector_year_ticker_volume_RDD = stock_join.map(f=lambda l: ((l[4], l[2], l[0]), l[6]))

# (sector, year , ticker), sum_volume)
sector_year_ticker_volume_sum_RDD = sector_year_ticker_volume_RDD.reduceByKey(func=lambda a, b: a + b)


def max_volume(el1, el2):
    if el1[1] > el2[1]:
        return el1
    return el2


# (sector, year), (ticker, max_volume)
sector_year_ticker_volume_sumMAX_RDD = sector_year_ticker_volume_sum_RDD.map(
    f=lambda l: ((l[0][0], l[0][1]), (l[0][2], l[1]))) \
    .reduceByKey(func=max_volume)

# PUNTO A e B
# (sector, year , ticker), (date,close)
sector_year_ticker_date_close_RDD = stock_join.map(f=lambda l: ((l[4], l[2], l[0]), (l[3], l[5])))


def min_date(v1, v2):
    date1 = v1[0]
    date2 = v2[0]
    if date1 < date2:
        return v1
    return v2


def max_date(v1, v2):
    date1 = v1[0]
    date2 = v2[0]
    if date1 > date2:
        return v1
    return v2


# (sector, year , ticker), (date_min,close)
sector_year_ticker_datemin_close_RDD = sector_year_ticker_date_close_RDD.reduceByKey(func=min_date)
# (sector, year , ticker), (date_max,close)
sector_year_ticker_datemax_close_RDD = sector_year_ticker_date_close_RDD.reduceByKey(func=max_date)

# Punto A Continue
# (sector, year , ticker), (date_min,close) -->>
# (sector, year ), close_min_date) -->>
# (sector, year ), sum_close_min_date)
sector_year_datemin_sum_close_RDD = sector_year_ticker_datemin_close_RDD.map(f=lambda l: ((l[0][0], l[0][1]), l[1][1]))\
    .reduceByKey(func=lambda a, b: a + b)

# (sector, year ), close_max_date)
# (sector, year ), sum_close_max_date)
sector_year_datemax_sum_close_RDD = sector_year_ticker_datemax_close_RDD.map(f=lambda l: ((l[0][0], l[0][1]), l[1][1]))\
    .reduceByKey(func=lambda a, b: a + b)


def var_percent_sector(el):
    close_price_min_date = float(el[0])
    close_price_max_date = float(el[1])
    var_percent = ((close_price_max_date - close_price_min_date) / close_price_min_date) * 100
    return var_percent


# (sector, year ), (sum_close_max_date,sum_close min_date))
# (sector,year ), var_percent)
sector_year_var_percent_RDD = sector_year_datemin_sum_close_RDD.join(sector_year_datemax_sum_close_RDD) \
    .mapValues(f=var_percent_sector)


# PUNTO B continue

def var_percent_ticker(el):
    close_price_min_date = float(el[0][1])
    close_price_max_date = float(el[1][1])
    var_percent = ((close_price_max_date - close_price_min_date) / close_price_min_date) * 100
    return var_percent


# ((sector, year , ticker), (date_min,close),(date_max,close))
# (sector, year , ticker), percetage_variation)
sector_year_ticker_var_percent_RDD = sector_year_datemin_sum_close_RDD.join(sector_year_datemax_sum_close_RDD) \
    .mapValues(f=var_percent_ticker)


def max_var_percent(el1, el2):
    pv1 = el1[0]
    pv2 = el2[0]
    if pv1 > pv2:
        return pv1
    return pv2


# (sector,year,ticker), max_var_percent)
# (sector,year),(ticker, max_var_percent)
sector_year_ticker_max_var_percent_RDD = sector_year_ticker_var_percent_RDD.map(
    f=lambda l: ((l[0][0], l[0][1]), (l[0][2], l[1]))) \
    .reduceByKey(func=max_var_percent)


def pretty_print(elem):
    output = ""
    sector = str(elem[0])
    year = str(elem[1])
    sector_percentage_variation = str(elem[2]) + "%"
    ticker_max_pv = str(elem[3])
    max_pv = str(elem[4]) + "%"
    ticker_max_volume = str(elem[5])
    max_volume = str(elem[6])
    output = output + "Sector: " + sector + ", "
    output = output + "Year: " + year + ", "
    output = output + "Sector Percentage Variation: " + sector_percentage_variation + ", "
    output = output + "Ticker Max Percentage Variation: " + ticker_max_pv + " " + "Percentage Variation:" + max_pv + ", "
    output = output + "Ticker Max Transaction Volume: " + ticker_max_volume + " " + "Volume:" + max_volume
    return output


# (sector,year),(ticker,max_var_percent),var_percent_sector)
# (sector,year),(ticker,max_var_percent,var_percent_sector))
# (sector,year),((ticker,max_var_percent,var_percent_sector),(ticker,max_volume))
# (sector,year),(ticker,max_var_percent,var_percent_sector, ticker,max_volume)
final_join_RDD = sector_year_ticker_max_var_percent_RDD.join(sector_year_var_percent_RDD) \
    .mapValues(f=lambda l: (l[0][0], l[0][1], l[1])) \
    .join(sector_year_ticker_volume_sumMAX_RDD) \
    .mapValues(f=lambda l: (l[0][0], l[0][1], l[1][0][2], l[1][0][0], l[1][0][1], l[1][1][0], l[1][1][1])) \
    .sortBy(keyfunc=lambda element: element[0]) \
    .map(f=pretty_print)

final_join_RDD.saveAsTextFile(output_filepath)
