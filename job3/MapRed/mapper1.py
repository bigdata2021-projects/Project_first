#!/usr/bin/env python3
import datetime
import sys

for line in sys.stdin:
    line = line.strip()
    ticker, date_act, close_act, name = line.split(",")
    if ticker == "ticker":
        continue
    date_new = datetime.datetime.strptime(date_act, '%Y-%m-%d')
    year = date_new.year
    if year == 2017:
        print("%s,%s,%s,%s" % (ticker, name, close_act, date_act))
