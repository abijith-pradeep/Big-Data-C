#!/usr/bin/python
import sys,os

inp = os.environ['mapreduce_map_input_file']

for line in sys.stdin:
    line = line.strip()

    if "medallion" in line:
        continue
    columns = line.split(',') # split line into parts
    # the data is messy, only read those having correct column count
    if "trips" in inp:
        try:
            col2_key=columns[:3]
            col2_key.append(columns[5])
            col2_key=','.join(col2_key)
            col2_val=columns[3:5]
            col2_val.extend(columns[6:])
            col2_val=','.join(col2_val)
            print col2_key+"\t"+col2_val+" trip"
        except ValueError:
            pass
    if "fares" in inp:
        try:
            col_key=','.join(columns[:4])
            col_val=','.join(columns[4:])
            print col_key+"\t"+col_val+" fare"
        except ValueError:
            pass