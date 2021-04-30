# !/usr/bin/env python3
import datetime
import sys

for line in sys.stdin:
    line = line.strip()
    ticker, open_act, close_act, adj_close, lowThe_act, highThe_act, volume_act, date = line.split(",")
    open_act1 = float(open_act)
    close_act1 = float(close_act)
    lowThe_act1 = float(lowThe_act)
    highThe_act1 = float(highThe_act)
    volume_act1 = float(volume_act)
    # dateStr = datetime.datetime.strftime(date, '%Y-%m-%d')
    print("%s\t%f\t%f\t%f\t%f\t%f\t%s" % (ticker, open_act1, close_act1, lowThe_act1, highThe_act1, volume_act1, date));
