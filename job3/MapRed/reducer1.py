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

    if (ticker, month) not in action_maxclose_dictionary.keys():
        action_maxclose_dictionary[ticker, month] = [name, date_new, close_act]
    elif day >= action_maxclose_dictionary[ticker, month][1].day:
        action_maxclose_dictionary[ticker, month] = [name, date_new, close_act]

for ticker, month in action_minclose_dictionary.keys():
    if (ticker, month) in action_maxclose_dictionary.keys():
        name = action_minclose_dictionary[ticker, month][0]
        close_min = action_minclose_dictionary[ticker, month][2]
        close_max = action_maxclose_dictionary[ticker, month][2]
        action_varmonth_dictionary[ticker, month] = [name, ((close_max - close_min) / close_min) * 100]

for ticker, month in action_varmonth_dictionary.keys():
    print(ticker + "," + str(month) + "," + str(action_varmonth_dictionary[ticker,month][0]) + "," + str(action_varmonth_dictionary[ticker,month][1]))


