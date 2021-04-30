# !/usr/bin/env python3
import datetime
import sys

action_dictionary = {}

for line in sys.stdin:
    line = line.strip()
    ticker, open_str, close_str, lowThe_str, highThe_str, volume_str, date = line.split("\t")
    dateNew = datetime.datetime.strptime(date, '%Y-%m-%d')
    open_act = float(open_str)
    close = float(close_str)
    lowThe = float(lowThe_str)
    highThe = float(highThe_str)
    volume = float(volume_str)
    if ticker not in action_dictionary:

        var_perc = ((close - close) / close) * 100
        action_dictionary[ticker] = [dateNew, dateNew, var_perc, close, close, highThe, lowThe]

    else:
        feature = action_dictionary[ticker]
        if dateNew < feature[0]:
            feature[0] = dateNew
            feature[2] = ((feature[4] - close) / close) * 100
            feature[3] = close
            action_dictionary[ticker] = feature
        if dateNew > feature[1]:
            feature[1] = dateNew
            feature[2] = ((close - feature[3]) / feature[3]) * 100
            feature[4] = close
            action_dictionary[ticker] = feature
        if highThe > feature[5]:
            feature[5] = highThe
        if lowThe < feature[6]:
            feature[6] = lowThe

for ticker in action_dictionary.keys():
    print(ticker + str(action_dictionary[ticker]))