#!/usr/bin/env python3
import datetime
import sys

for line in sys.stdin:
    line = line.strip()
    ticker, month, name, var_per = line.split(",")
    print("%s,%s,%s,%s" % (ticker, month, name, var_per))