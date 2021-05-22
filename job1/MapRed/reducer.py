#!/usr/bin/env python3
import datetime
import sys

action_dic={}
date_min_dic={}
date_max_dic={}
highThe_dic={}
lowthe_dic={}
output_dic = {}


for line in sys.stdin:
    line = line.strip()
    ticker, open_str, close_str, lowThe_str, highThe_str, volume_str, date_string = line.split(",")
    if ticker == "ticker":
        continue
    dateNew = datetime.datetime.strptime(date_string, '%Y-%m-%d')
    open_act = float(open_str)
    close = float(close_str)
    lowThe = float(lowThe_str)
    highThe = float(highThe_str)
    volume = float(volume_str)


    if ticker not in date_min_dic.keys():
        date_min_dic[ticker] = [dateNew,close]
    elif dateNew < date_min_dic[ticker][0]:
            date_min_dic[ticker]= [dateNew,close]

    if ticker not in date_max_dic.keys():
        date_max_dic[ticker]=[dateNew,close]
    elif dateNew > date_max_dic[ticker][0]:
        date_max_dic[ticker]= [dateNew,close]

    if ticker not in highThe_dic.keys():
        highThe_dic[ticker] = highThe
    elif highThe > highThe_dic[ticker]:
         highThe_dic[ticker] = highThe

    if ticker not in lowthe_dic.keys():
        lowthe_dic[ticker] = lowThe
    elif lowThe < lowthe_dic[ticker]:
         lowthe_dic[ticker] = lowThe


for ticker in date_min_dic.keys():
     if ticker in date_max_dic.keys():
         close_min = date_min_dic[ticker][1]
         close_max = date_max_dic[ticker][1]
         date_min = date_min_dic[ticker][0]
         date_max = date_max_dic[ticker][0]
         var_max = ((close_max - close_min)/close_min)*100
     if ticker not in output_dic.keys():
         output_dic[ticker] = [date_min] + [date_max] + [var_max]
     else:
         continue
     if ticker in highThe_dic.keys():
        output_dic[ticker] = output_dic[ticker] + [highThe_dic[ticker]]
     if ticker in lowthe_dic.keys():
        output_dic[ticker] = output_dic[ticker] + [lowthe_dic[ticker]]


for ticker, value in sorted(output_dic.items(), key=lambda value: value[1][1], reverse=True):
    output_dic[ticker][0] = output_dic[ticker][0].strftime('%Y-%m-%d')
    output_dic[ticker][1] = output_dic[ticker][1].strftime('%Y-%m-%d')
    print("#######################################")
    print(ticker)
    print("#######################################")
    print("Data prima quotazione: " + str(output_dic[ticker][0])
          + "\n" + "Data ultima quotazione: " + str(output_dic[ticker][1])
          + "\n" + "Variazione percentuale: " + str(output_dic[ticker][2])
          + "\n" + "Prezzo massimo: " + str(output_dic[ticker][3])
          + "\n" + "Prezzo minimo: " + str(output_dic[ticker][4])
          )






