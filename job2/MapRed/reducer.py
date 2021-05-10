#!/usr/bin/env python3
import datetime
import sys

# sector and min date values POINT A
sector_mindate_dictionary = {}
# sector and max date values POINT A
sector_maxdate_dictionary = {}
# min quotation of sector for a year POINT A
quotation_min_dictionary = {}
quotation_max_dictionary = {}
variation_dictionary = {}

# sector and max volume POINT C
sector_maxvolume_dictionary = {}
sector_sumvolume_dictionary = {}

# ticker and var per year POINT B
sector_tickervar_dictionary = {}
sector_tickervarmax_dictionary = {}

for line in sys.stdin:
    line = line.strip()
    ticker, date_string, close_act, volume, sector = line.split(",")
    date_temp = datetime.datetime.strptime(date_string, '%Y-%m-%d')
    year = date_temp.year
    volume = float(volume)
    # Point A in FOR
    if (sector, ticker, year) not in sector_mindate_dictionary.keys():
        sector_mindate_dictionary[sector, ticker, year] = [date_string, close_act]
    elif (sector, ticker, year) not in sector_maxdate_dictionary.keys():
        sector_maxdate_dictionary[sector, ticker, year] = [date_string, close_act]
    else:
        date_min = datetime.datetime.strptime(sector_mindate_dictionary[sector, ticker, year][0], '%Y-%m-%d')
        date_max = datetime.datetime.strptime(sector_maxdate_dictionary[sector, ticker, year][0], '%Y-%m-%d')
        if date_temp <= date_min:
            sector_mindate_dictionary[sector, ticker, year] = [date_string, close_act]
        elif date_temp >= date_max:
            sector_maxdate_dictionary[sector, ticker, year] = [date_string, close_act]
    # Point C in FOR
    if (sector, ticker, year) not in sector_sumvolume_dictionary.keys():
        sector_sumvolume_dictionary[sector, ticker, year] = volume
    else:
        sector_sumvolume_dictionary[sector, ticker, year] += volume

# POINT A: Sommo le close in data minore e le putto nel dizionario quotation_min
sector_temp = ""
for sector_key, ticker_key, year in sector_mindate_dictionary.keys():
    if (sector_key, year) not in quotation_min_dictionary.keys():
        quotation_min_dictionary[sector_key, year] = float(sector_mindate_dictionary[sector_key, ticker_key, year][1])
    elif sector_key != sector_temp:
        quotation_min_dictionary[sector_key, year] = float(sector_mindate_dictionary[sector_key, ticker_key, year][1])
        sector_temp = sector_key
    else:
        quotation_min_dictionary[sector_key, year] += float(sector_mindate_dictionary[sector_key, ticker_key, year][1])

# POINT A: Sommo le close in data maggiore e le putto nel dizionario quotation_max
sector_temp = ""
for sector_key, ticker_key, year_key in sector_maxdate_dictionary.keys():
    year = datetime.datetime.strptime(sector_maxdate_dictionary[sector_key, ticker_key, year_key][0], '%Y-%m-%d').year
    if (sector_key, year) not in quotation_max_dictionary.keys():
        quotation_max_dictionary[sector_key, year] = float(sector_maxdate_dictionary[sector_key,
                                                                                     ticker_key, year_key][1])
    elif sector_key != sector_temp:
        quotation_max_dictionary[sector_key, year] = float(sector_maxdate_dictionary[sector_key,
                                                                                     ticker_key, year_key][1])
        sector_temp = sector_key
    else:
        quotation_max_dictionary[sector_key, year] += float(sector_maxdate_dictionary[sector_key,
                                                                                      ticker_key, year_key][1])

# POINT A: Calcolo variazione quotazione
for sector_key, year in quotation_max_dictionary.keys():
    for sector_1key, year1 in quotation_min_dictionary.keys():
        if sector_key == sector_1key and year == year1:
            q_min = quotation_min_dictionary[sector_key, year]
            q_max = quotation_max_dictionary[sector_1key, year1]
            variation_dictionary[sector_key, year] = ((q_max - q_min) / q_min) * 100
# POINT B
for sector_key, ticker_key, year_key in sector_mindate_dictionary.keys():
    for sector1_key, ticker1_key, year1_key in sector_maxdate_dictionary.keys():
        if ticker_key == ticker1_key and year_key == year1_key:
            close_min = sector_mindate_dictionary[sector_key, ticker_key, year_key][1]
            close_max = sector_maxdate_dictionary[sector1_key, ticker1_key, year1_key][1]
            sector_tickervar_dictionary[sector_key,
                                        ticker_key, year_key] = ((float(close_max)
                                                                  - float(close_min)) / float(close_min)) * 100
# POINT B
for sector_key, ticker_key, year_key in sector_tickervar_dictionary.keys():
    variation = float(sector_tickervar_dictionary[sector_key, ticker_key, year_key])
    if (sector_key, year_key) not in sector_tickervarmax_dictionary.keys():
        sector_tickervarmax_dictionary[sector_key, year_key] = [ticker_key, variation]
        # var_temp_dictionary[year_key] = variation
    elif sector_tickervarmax_dictionary[sector_key, year_key][1] <= variation:
        sector_tickervarmax_dictionary[sector_key, year_key] = [ticker_key, variation]
# Point C in FOR
for (sector, ticker, year) in sector_sumvolume_dictionary.keys():
    if (sector, year) not in sector_maxvolume_dictionary.keys():
        sector_maxvolume_dictionary[sector, year] = [sector_sumvolume_dictionary[sector, ticker, year], ticker]
    elif sector_sumvolume_dictionary[sector, ticker, year] >= sector_maxvolume_dictionary[sector, year][0]:
        sector_maxvolume_dictionary[sector, year] = [sector_sumvolume_dictionary[sector, ticker, year], ticker]
"""
# return  POINT A
for key1, key2 in variation_dictionary.keys():
    print(key1 + ", " + str(key2) + ", " + str(variation_dictionary[key1, key2]))

# return POINT C
for key1, key2 in sector_maxvolume_dictionary.keys():
    print(key1 + ", " + str(key2) + ", "
          + str(sector_maxvolume_dictionary[key1, key2][0]) + ", "
          + str(sector_maxvolume_dictionary[key1, key2][1]))
# return POINT B
for key1, key2 in sector_tickervarmax_dictionary.keys():
    print(key1 + ", " + str(key2) + ", "
          + str(sector_tickervarmax_dictionary[key1, key2][0]) + ", "
          + str(sector_tickervarmax_dictionary[key1, key2][1]))
"""
for (key1, key2), value in sorted(sector_maxvolume_dictionary.items()):
    print("###########################################################\n")
    print("settore: " + key1 +
          " anno: " + str(key2) + "\n")
    print("###########################################################\n")
    print("a)variazione quotazione settore: \n" +
          str(variation_dictionary[key1, key2]) + "\n")
    print("b)azione del settore con variazione massima: \n" +
          str(sector_tickervarmax_dictionary[key1, key2][0]) + ", "
          + str(sector_tickervarmax_dictionary[key1, key2][1]) + "\n")
    print("c)azione del settore con volumi massimi: \n" +
          str(sector_maxvolume_dictionary[key1, key2][1]) + ","
          + str(sector_maxvolume_dictionary[key1, key2][0]) + "\n")
