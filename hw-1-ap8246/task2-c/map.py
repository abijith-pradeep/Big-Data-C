#!/usr/bin/python
import sys

for line in sys.stdin:
    line = line.strip()

    key,val = line.split('\t') # split line into parts
    passenger_count = float(val.strip().split(',')[3])
    print "%d\t%d" % (passenger_count,1)