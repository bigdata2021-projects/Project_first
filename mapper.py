#!/usr/bin/env python3
import datetime
import sys

for line in sys.stdin:
    line = line.strip()
    ticker, open_act, close_act, adj_close, lowThe_act, highThe_act, volume_act, date_act = line.split(",")
    print("%s,%s,%s,%s,%s,%s,%s" % (ticker, open_act, close_act, lowThe_act, highThe_act, volume_act, date_act))
