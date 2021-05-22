#!/usr/bin/env python3
import datetime
import sys

action_dictionary = {}
name_dictionary = {}
close_dictionary = {}

for line in sys.stdin:
    line = line.strip()
    ticker, close_act, name, date_string, temp = line.split(",")

    if temp == "0":
        close_dictionary[ticker, date_string] = close_act
    else:
        name_dictionary[ticker] = name

for ticker_key, data_key in close_dictionary.keys():
    if ticker_key in name_dictionary.keys():
        action_dictionary[ticker_key, data_key] = [close_dictionary[ticker_key, data_key],
                                                       name_dictionary[ticker_key]]

for ticker, data in action_dictionary.keys():
    print(ticker + "," + data + "," + str(action_dictionary[ticker, data][0]) +
          "," + action_dictionary[ticker, data][1])
