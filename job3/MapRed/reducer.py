#!/usr/bin/env python3
import datetime
import sys

action_minclose_dictionary = {}
action_maxclose_dictionary = {}
action_varmonth_dictionary = {}
action_similarity_dictionary = {}
action_print_dictionary = {}
for line in sys.stdin:
    line = line.strip()
    ticker, name, close_act, date_act = line.split(",")
    date_new = datetime.datetime.strptime(date_act, '%Y-%m-%d')
    month = date_new.month
    day = date_new.day
    close_act = float(close_act)
    if (ticker, month) not in action_minclose_dictionary.keys():
        action_minclose_dictionary[ticker, month] = [name, date_new, close_act]
    elif day <= action_minclose_dictionary[ticker, month][1].day:
        action_minclose_dictionary[ticker, month] = [name, date_new, close_act]

    if ticker not in action_maxclose_dictionary.keys():
        action_maxclose_dictionary[ticker, month] = [name, date_new, close_act]
    elif day >= action_maxclose_dictionary[ticker, month][1]:
        action_maxclose_dictionary[ticker, month] = [name, date_new, close_act]

for ticker, month in action_minclose_dictionary.keys():
    for ticker1, month1 in action_maxclose_dictionary.keys():
        if ticker == ticker1 and month == month1:
            name = action_minclose_dictionary[ticker, month][0]
            close_min = action_minclose_dictionary[ticker, month][2]
            close_max = action_maxclose_dictionary[ticker, month][2]
            action_varmonth_dictionary[ticker, month] = [name, ((close_max - close_min) / close_min) * 100]

for ticker, month in action_varmonth_dictionary.keys():
    for ticker1, month1 in action_varmonth_dictionary.keys():
        if month == month1 and ticker != ticker1:
            name = action_varmonth_dictionary[ticker, month][0]
            name1 = action_varmonth_dictionary[ticker1, month1][0]
            var = action_varmonth_dictionary[ticker, month][1]
            var1 = action_varmonth_dictionary[ticker1, month1][1]
            if abs(var - var1) <= 1 and (ticker1, ticker, month) not in action_similarity_dictionary.keys():
                action_similarity_dictionary[ticker, ticker1, month] = [name, var, name1, var1]
"""
for ticker, month in sorted(action_varmonth_dictionary.keys(), key=lambda key: key[1]):
    print(ticker + "\t" + str(month) + "\t" + str(action_varmonth_dictionary[ticker, month]))
"""
for ticker, ticker1, month in action_similarity_dictionary:
    if (ticker, ticker1) not in action_print_dictionary.keys():
        action_print_dictionary[ticker, ticker1] = [action_similarity_dictionary[ticker, ticker1, month] + [month]]
    else:
        action_print_dictionary[ticker, ticker1] = action_print_dictionary[ticker, ticker1] + \
                                                   [action_similarity_dictionary[ticker, ticker1, month] + [month]]
"""
for ticker, ticker1, month in action_similarity_dictionary:
    print("\n\n\n" + str(month) + "\t" + ticker
          + "\t" + str(action_similarity_dictionary[ticker, ticker1, month][0]) + "\t" + ticker1 +
          "\t" + str(action_similarity_dictionary[ticker, ticker1, month][1]))
"""
for ticker, ticker1 in action_print_dictionary.keys():
    print("\n####################################################\n")
    print("{" + action_print_dictionary[ticker, ticker1][0][0] + "," +
          action_print_dictionary[ticker, ticker1][0][2] + "}: \n")
    for elem in action_print_dictionary[ticker, ticker1]:
        print("mese:" + str(elem[4]) + "\t" + str(elem[0]) + ":"
              + str(elem[1]) + "\t" + str(elem[2]) + ":" + str(elem[3]) + "\n")
