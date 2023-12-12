#!/usr/bin/python
import sys

for line in sys.stdin:
    line = line.strip()

    key,val = line.split('\t') # split line into parts
    total_amount = float(val.strip().split(',')[-1])
    if total_amount<=15:
       print "%d\t%d" % (15,1)
    else:
      continue