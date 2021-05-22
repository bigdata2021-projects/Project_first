#!/usr/bin/env python3
import datetime
import sys

action_dictionary = {}
sector_dictionary = {}
close_dictionary = {}

for line in sys.stdin:
    line = line.strip()
    ticker, close_act, volume, sector, date_string, temp = line.split(",")

    if temp == "0":
        close_dictionary[ticker, date_string] = [close_act, volume]
    else:
        sector_dictionary[ticker] = sector


for ticker1_key, data_key in close_dictionary.keys():
    if ticker1_key in sector_dictionary.keys():
        action_dictionary[ticker1_key, data_key] = close_dictionary[ticker1_key, data_key] + [
            sector_dictionary[ticker1_key]]

for ticker, data in action_dictionary.keys():
    print(ticker + "," + data + "," + str(action_dictionary[ticker, data][0]) +
          "," + str(action_dictionary[ticker, data][1]) +
          "," + str(action_dictionary[ticker, data][2]))
