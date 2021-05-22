#!/usr/bin/env python3
import datetime
import sys

months = {1: 'JAN', 2: 'FEB', 3: 'MAR', 4: 'APR', 5: 'MAY', 6: 'JUN', 7: 'JUL',
          8: 'AUG', 9: 'SEP', 10: 'OCT', 11: 'NOV', 12: 'DEC'}


action_varmonth_dictionary = {}
action_similarity_dictionary = {}
action_print_dictionary = {}

for line in sys.stdin:
    line = line.strip()
    ticker, month, name, var_per = line.split(",")
    if month not in action_varmonth_dictionary.keys():
       action_varmonth_dictionary[month] = dict()
    if ticker not in action_varmonth_dictionary[month]:
       action_varmonth_dictionary[month][ticker] = [name, var_per]
    

for month in action_varmonth_dictionary.keys():
    ticker_months = action_varmonth_dictionary[month].items()
    for ticker in ticker_months:
        for ticker1 in ticker_months:
            if ticker[0] != ticker1[0]:
               name = ticker[1][0]
               name1 = ticker1[1][0]
               var = float(ticker[1][1])
               var1 = float(ticker1[1][1])
               if abs(var - var1) <= 1 and (ticker[0], ticker1[0], month) not in action_similarity_dictionary.keys() and (ticker1[0], ticker[0], month) not in action_similarity_dictionary.keys():
                action_similarity_dictionary[ticker[0], ticker1[0], month] = [name, var, name1, var1]

for ticker, ticker1, month in action_similarity_dictionary.keys():
    if (ticker, ticker1) not in action_print_dictionary.keys():
        action_print_dictionary[ticker, ticker1] = [action_similarity_dictionary[ticker, ticker1, month] + [month]]
    else:
        action_print_dictionary[ticker, ticker1] = action_print_dictionary[ticker, ticker1] + \
                                                   [action_similarity_dictionary[ticker, ticker1, month] + [month]]



for ticker, ticker1 in action_print_dictionary.keys():
    print("\n####################################################\n")
    print("{" + action_print_dictionary[ticker, ticker1][0][0] + "," +
          action_print_dictionary[ticker, ticker1][0][2] + "}: \n")
    for elem in action_print_dictionary[ticker, ticker1]:
        print("mese:" + str(months[int(elem[4])]) + "\t" + str(elem[0]) + ":"
              + str(elem[1]) + "\t" + str(elem[2]) + ":" + str(elem[3]) + "\n")
