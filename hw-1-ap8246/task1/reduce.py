#!/usr/bin/python

import sys

fares_list = []
trips_list = []

prev_key = ""

# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:

    key, val = line.strip().split("\t", 1)
    if key!=prev_key:
       if prev_key:
          for trip in trips_list:
              for fare in fares_list:
                  print prev_key +'\t'+trip+','+fare
          
          trips_list = []
          fares_list = []

    if "trip" in val:
       val = val.replace(" trip","")
       trips_list.append(val)
    elif "fare" in val:
       val = val.replace(" fare","")
       fares_list.append(val)

    prev_key = key

for trip in trips_list:
    for fare in fares_list:
        print prev_key + '\t' + trip + ',' + fare