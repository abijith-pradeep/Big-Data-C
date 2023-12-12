#!/usr/bin/python

import sys


current_key = ""
rev = 0
tip = 0

# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:

    key,val = line.strip().split('\t',1)
    
    val_rev = float(val.strip().split(',')[0])
    val_tip = float(val.strip().split(',')[1])
    if key != current_key:
       if current_key!="":
          print ("%s\t%.2f,%.2f" % (current_key,rev,tip))
       rev=0
       tip=0
    rev+= val_rev
    tip+= val_tip
    current_key = key

print ("%s\t%.2f,%.2f" % (current_key,rev,tip))