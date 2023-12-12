#!/usr/bin/python

import sys


#current_key = ""
current_sum = 0

# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:

    key,val = line.strip().split('\t',1)
    try:
       val = int(val)
    except ValueError:
       continue
    current_sum+=val

print current_sum