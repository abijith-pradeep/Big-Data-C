#!/usr/bin/python

import sys


current_key = ""
current_sum = 0

# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:

    key,val = line.strip().split('\t',1)
    try:
       val = int(val)
    except ValueError:
       continue

    if key == current_key:
       current_sum += val
    else:
       if current_key:
           print "%s\t%d" % (current_key, current_sum)
       current_key = key
       current_sum = val

if current_key ==key:
   print "%s\t%d" % (current_key,current_sum)