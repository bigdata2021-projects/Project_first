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

"""
Un job in grado di generare le coppie di aziende che si somigliano 
(sulla base di una soglia scelta a piacere) in termini di variazione percentuale mensile
 nell’anno 2017 mostrando l’andamento mensile delle due aziende 
 (es. Soglia=1%, coppie: 1:{Apple, Intel}: GEN: Apple +2%, Intel +2,5%, 
 FEB: Apple +3%, Intel +2,7%, MAR: Apple +0,5%, Intel +1,2%, …; 
 2:{Amazon, IBM}: GEN: Amazon +1%, IBM +0,5%, FEB: Amazon +0,7%, IBM +0,5%, 
 MAR: Amazon +1,4%, IBM +0,7%, ..)
"""

lines_RDD_1 = spark.sparkContext.textFile(input_filepath1)

lines_RDD_2 = spark.sparkContext.textFile(input_filepath2)

stock_prices_RDD = lines_RDD_1.map(f=lambda line: line.strip().split(",")) \
    .filter(f=lambda l: datetime.strptime(l[7], '%Y-%m-%d').year == 2017) \
    .filter(lambda line: line[0] != "ticker") \
    .map(f=lambda pair: (pair[0], pair))

stock_RDD = lines_RDD_2.map(f=lambda line: line.strip().split(",")) \
    .map(f=lambda pair: (pair[0], pair[2]))


def month_ticker(element):
    ticker = element[0]
    l = element[1][0]
    name = element[1][1]
    date = datetime.strptime(l[7], '%Y-%m-%d')
    month = date.month
    close_act = l[2]
    return (month, ticker), (close_act, date, name)


stock_join = stock_prices_RDD.join(stock_RDD).map(f=month_ticker)


def min_date(v1, v2):
    date1 = v1[1]
    date2 = v2[1]
    if date1 < date2:
        return v1
    return v2


def max_date(v1, v2):
    date1 = v1[1]
    date2 = v2[1]
    if date1 > date2:
        return v1
    return v2


min_dates_RDD = stock_join.reduceByKey(func=min_date)
max_dates_RDD = stock_join.reduceByKey(func=max_date)

join_date_RDD = min_dates_RDD.join(max_dates_RDD)


# ((month, ticker), ((close, min_date, name), (close, max_date, name)) -> ((month, ticker), (name, percent_variation))

def var_percent_act(el):
    month = el[0][0]
    ticker = el[0][1]
    close_price_min_date = float(el[1][0][0])
    close_price_max_date = float(el[1][1][0])
    name = el[1][0][2]
    var_percent = ((close_price_max_date - close_price_min_date) / close_price_min_date) * 100
    return month, (ticker, name, var_percent)


# (month ,(ticker, name ,percent_variation))
var_act_RDD = join_date_RDD.map(f=var_percent_act)

# (month , [(ticker1,name,var),(ticker1,name,var), ..]
month_var_percent_RDD = var_act_RDD.groupByKey().mapValues(list)


def max_treshold_similiar(var1, var2):
    sim = abs(var1 - var2)
    return sim < 1.0


def similiar_pairs(l):
    comb = list(combinations(l, 2))
    sim_pairs = list(filter(lambda pair: max_treshold_similiar(pair[0][2], pair[1][2]), comb))
    return sim_pairs


# (month ,[((ticker1,name,var_percent),(ticker2,name,var_percent),....)]
month_similiar_pairs_RDD = month_var_percent_RDD.mapValues(f=similiar_pairs)


def to_print(element):
    output = []
    month = element[0]
    l = element[1]
    for el in l:
        ticker1 = el[0][0]
        name1 = el[0][1]
        var_percent1 = el[0][2]
        ticker2 = el[1][0]
        name2 = el[1][1]
        var_percent2 = el[1][2]
        output.append((month, ticker1, name1, var_percent1, ticker2, name2, var_percent2))
    return output


# (ticker1,ticker) & (month,name1,var_percent1,name2,var_percent2)
sim_pairs_RDD = month_similiar_pairs_RDD.flatMap(f=to_print).map(
    f=lambda l: ((l[1], l[4]), (l[0], l[2], l[3], l[5], l[6])))

similiar_pairs_final_RDD = sim_pairs_RDD.groupByKey()

number_to_month = {1: "GEN", 2: "FEB",
                   3: "MAR", 4: "APR",
                   5: "MAY", 6: "JUN",
                   7: "JUL", 8: "AUG",
                   9: "SEP", 10: "OCT",
                   11: "NOV", 12: "DEC"}


def pretty_print(element):
    ticker1 = element[0][0]
    ticker2 = element[0][1]
    l = element[1]
    # sorting by month
    l_ordered = sorted(l, key=lambda x: x[0])
    output = ""
    output = output + "{" + str(ticker1) + "," + str(ticker2) + "}" + ": "
    for x in l_ordered:
        # (month , name1, percent_variation1, name2, percent_variation2)
        output = output + number_to_month[int(x[0])] + ": "
        output = output + str(x[1]) + " " + str(x[2]) + "%" + ", "
        output = output + str(x[3]) + " " + str(x[4]) + "%" + "; "
    output = output[:-2]
    return output


output_RDD = similiar_pairs_final_RDD.map(f=pretty_print)
output_RDD.saveAsTextFile(output_filepath)
