#!/usr/bin/python
import sys

for line in sys.stdin:
    line = line.strip()

    key,val = line.split('\t') # split line into parts
    fare_amount = float(val.strip().split(',')[-6])
    if fare_amount>=0 and fare_amount<=20:
       print "%s,%s\t%d"%(0,20,1)
    elif fare_amount>=20.01 and fare_amount<=40:
       print "%s,%s\t%d"%(20.01,40,1)
    elif fare_amount>=40.01 and fare_amount<=60:
       print "%s,%s\t%d"%(40.01,60,1)
    elif fare_amount>=60.01 and fare_amount<=80:
       print "%s,%s\t%d"%(60.01,80,1)
    else:
       print "%s,%s\t%d"%(80.01,'infinite',1)