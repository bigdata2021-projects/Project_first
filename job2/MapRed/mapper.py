#!/usr/bin/env python3
import datetime
import sys

for line in sys.stdin:
    line = line.strip()
    ticker, date_string, close_act, volume, sector = line.split(",")
    print("%s,%s,%s,%s,%s" %
          (ticker, date_string, close_act, volume, sector))
