#!/usr/bin/env python3
import datetime
import sys

date_min = datetime.datetime.strptime('2009-01-01', '%Y-%m-%d')
date_max = datetime.datetime.strptime('2018-12-31', '%Y-%m-%d')
for line in sys.stdin:
    line = line.strip()
    line1 = line.split(",")
    temp = 0
    close_act = float(0)
    date_string = "-"
    name = "-"
    ticker = "-"
    if line1[0] == "ticker":
        continue
    if len(line1) == 8:
        date_temp = datetime.datetime.strptime(line1[7], '%Y-%m-%d')
        if date_max >= date_temp >= date_min:
            ticker = line1[0]
            close_act = line1[2]
            close_act = float(close_act)
            date_string = line1[7]
            volume = int(line1[6])
            print("%s,%f,%s,%s,%i" % (ticker, close_act, name, date_string, temp))
    elif line1[3] != "N/A":
        ticker = line1[0]
        name = line1[2]
        temp = 1
        print("%s,%f,%s,%s,%i" % (ticker, close_act, name, date_string, temp))
