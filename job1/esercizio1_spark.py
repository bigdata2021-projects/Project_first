#!/usr/bin/env python3
"""spark application"""
import argparse
from pyspark.sql import SparkSession
from datetime import datetime
from itertools import combinations

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path1", type=str, help="Input file path1")
parser.add_argument("--output_path", type=str, help="Output folder path")
# parse arguments
args = parser.parse_args()
input_filepath1, output_filepath = args.input_path1, args.output_path
# initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Spark Job1") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

"""Un job che sia in grado di generare un report contenente, per ciascuna azione: (a) la data della prima quotazione, 
(b) la data dell’ultima quotazione, (c) la variazione percentuale della quotazione (differenza percentuale tra il 
primo e l’ultimo prezzo di chiusura presente nell’archivio), (d) il prezzo massimo e quello minimo. """

lines_RDD = spark.sparkContext.textFile(input_filepath1)

# (ticker_id,(open_act,close_act,lowThe_act,highThe_act,date_act))
stock_prices_RDD = lines_RDD.map(f=lambda line: line.strip().split(",")).filter(lambda line: line[0] != "ticker") \
    .map(f=lambda l: (l[0], (l[1], l[2], l[4], l[5], l[7])))


# Punto A
def mindate(value1, value2):
    date1 = value1[4]
    date2 = value2[4]
    if date1 < date2:
        return value1
    return value2


# Punto B
def maxdate(value1, value2):
    date1 = value1[4]
    date2 = value2[4]
    if date1 > date2:
        return value1
    return value2


# Punto D
def lowthe(value1, value2):
    lowthe1 = float(value1[2])
    lowthe2 = float(value2[2])
    if lowthe1 < lowthe2:
        return value1
    return value2


# Punto D
def highthe(value1, value2):
    highthe1 = float(value1[3])
    highthe2 = float(value2[3])
    if highthe1 > highthe2:
        return value1
    return value2


# Punto C

def varperc(val):
    close_min = float(val[0][1])
    close_max = float(val[1][1])
    var = ((close_max - close_min) / close_min) * 100
    return val[0][4], val[1][4], var


stock_prices_mindate_RDD = stock_prices_RDD.reduceByKey(func=mindate)
stock_prices_maxdate_RDD = stock_prices_RDD.reduceByKey(func=maxdate)
stock_prices_highThe_RDD = stock_prices_RDD.reduceByKey(func=highthe)
stock_prices_lowThe_RDD = stock_prices_RDD.reduceByKey(func=lowthe)
stock_prices_varperc_RDD = stock_prices_RDD.reduceByKey(func=varperc)

# join: (ticker_id,(open_act,close_mindate,lowThe_act,highThe_act,mindate), (
#                   open_act,open_act,close_maxdate,lowThe_act,highThe_act,maxdate)
# mapValues: (ticker_id,(mindate,maxdate,var_perc))
stock_prices_varperc_RDD = stock_prices_mindate_RDD.join(stock_prices_maxdate_RDD).mapValues(f=varperc)


# stock_prices_output_RDD = stock_prices_varperc_RDD.join(stock_prices_highThe_RDD).join(stock_prices_lowThe_RDD)
# stock_prices_output_RDD.saveAsTextFile(output_filepath)


def action_output(val):
    return val[0][0][0], val[0][0][1], val[0][0][2], val[1][2], val[0][1][3]


stock_prices_output_RDD = stock_prices_varperc_RDD.join(stock_prices_highThe_RDD).join(stock_prices_lowThe_RDD). \
    mapValues(f=action_output).sortBy(keyfunc=lambda element: element[1][1], ascending=False)


def pretty_print(item):
    ticker = item[0]
    date_min = item[1][0]
    date_max = item[1][1]
    var_perc = item[1][2]
    low = item[1][3]
    high = item[1][4]
    output = "####################\n" + ticker + "\n####################\n" + \
             "data minima: " + str(date_min) + "\n" + \
             "data massima: " + str(date_max) + "\n" + \
             "variazione percentuale: " + str(var_perc) + "\n" + \
             "prezzo minimo :" + str(low) + "\n" + \
             "prezzo massimo :" + str(high)
    return output


stock_prices_print_RDD = stock_prices_output_RDD.map(f=pretty_print)
stock_prices_print_RDD.saveAsTextFile(output_filepath)
