#!/usr/bin/env python3
import datetime
import sys

for line in sys.stdin:
    line = line.strip()
    ticker, open_act, close_act, adj_close, lowThe_act, highThe_act, volume_act, date_act = line.split(",")
    # open_act1 = float(open_act)
    # close_act1 = float(close_act)
    # lowThe_act1 = float(lowThe_act)
    # highThe_act1 = float(highThe_act)
    # volume_act1 = float(volume_act)
    # dateStr = datetime.datetime.strftime(date_act, '%Y-%m-%d')
    print("%s,%s,%s,%s,%s,%s,%s" % (ticker, open_act, close_act, lowThe_act, highThe_act, volume_act, date_act))
