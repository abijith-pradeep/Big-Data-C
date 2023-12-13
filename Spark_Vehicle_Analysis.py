#!/usr/bin/env python
# coding: utf-8



from pyspark.sql import SparkSession, Row
from pyspark import SparkConf, SparkContext
import os
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import plotly.express as px
import sys
import streamlit as st


os.environ['PYSPARK_PYTHON'] = 'python'
#os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#os.environ['SPARK_HOME']="C:/Users/321ni/spark-2.4.3-bin-hadoop2.7/"
#os.environ['PYSPARK_DRIVER_PYTHON']='jupyter'
#os.environ['PYSPARK_DRIVER_PYTHON_OPS']='lab'
#os.environ['PYSPARK_PYTHON']='python'

import findspark
findspark.find()

spark = SparkSession.builder.master("local[*]").config("spark.driver.memory", "4g").appName("sparkan").getOrCreate()


mvcdata2 = spark.read.format('csv').options(header='true',inferschema='true').load("vehicle.csv")



mvcdata2.createOrReplaceTempView("vData")

# print("***** Original Dataframe ******")

# print(mvcdata2.show())


# print("Total data count: ",mvcdata2.count())


# ## Grouping Vehicles as Large, Medium and Small

def vehicle_classifier(row):
    
    formatted_row = ','.join([key if type(key)==str else str(key) for key in row])
    
    if "bus" in formatted_row.lower() or "truck" in formatted_row.lower() or "freig" in formatted_row.lower()     or "ceme" in formatted_row.lower() or "fire" in formatted_row.lower() or "000" == formatted_row.lower()    or "concrete mixer" in formatted_row.lower() or "com" in formatted_row.lower() or "crane" in formatted_row.lower()    or "frieg" in formatted_row.lower() or "fdny" in formatted_row.lower() or "fedex" in formatted_row.lower()    or "dump" in formatted_row.lower() or "deli" in formatted_row.lower() or "exca" in formatted_row.lower()    or "flat" in formatted_row.lower() or "gar" in formatted_row.lower() or "lift boom" in formatted_row.lower()    or "mini schoo" in formatted_row.lower() or "omnib" in formatted_row.lower() or "power" in formatted_row.lower()    or "ic corporation" in formatted_row.lower() or "schoo" in formatted_row.lower() or "r/v" in formatted_row.lower()or    "rv" in formatted_row.lower() or "multi-wh" in formatted_row.lower() or "mta" in formatted_row.lower() or "mack" in    formatted_row.lower() or "sanit" in formatted_row.lower() or ("snow" in formatted_row.lower() and "snowm"     not in formatted_row.lower()) or "tank" in formatted_row.lower() or "lunch wagon" in formatted_row.lower()    or "trail" in formatted_row.lower() or "const" in formatted_row.lower() or "cat" in formatted_row.lower() or    "trlr" in formatted_row.lower() or "winne" in formatted_row.lower():
        return row + Row(VEHICLE_GROUP = "Heavy")
    elif "van" in formatted_row.lower() or "fleet" in formatted_row.lower() or "usps" in formatted_row.lower()     or "am" in formatted_row.lower() or ('4' in formatted_row.lower() and 'wh' in formatted_row.lower())    or "chassis" in formatted_row.lower() or "carry all" in formatted_row.lower() or "limo" in formatted_row.lower() or    "miniv" in formatted_row.lower() or "open body" in formatted_row.lower() or "pick" in formatted_row.lower() or    "pk" in formatted_row.lower() or "post" in formatted_row.lower() or "courier" in formatted_row.lower() or     "mail" in formatted_row.lower() or ("nypd" in formatted_row.lower() and "tow" in formatted_row.lower())     or "sub" in formatted_row.lower() or "stak" in formatted_row.lower() or "stree" in formatted_row.lower() or "sw"    in formatted_row.lower() or "tow" in formatted_row.lower() or "trac" in formatted_row.lower() or "sprin" in     formatted_row.lower() or "luv" in formatted_row.lower() or "gmc" in formatted_row.lower() or "lives"    in formatted_row.lower() or "almbulance" in formatted_row.lower() or "uti" in formatted_row.lower() or "yello"    in formatted_row.lower() or " yw po" == formatted_row.lower() or    ("u" in formatted_row.lower() and "ha" in formatted_row.lower()) or "ups" in formatted_row.lower() or     "utl" in formatted_row.lower():
        return row + Row(VEHICLE_GROUP = "Medium")
    elif "sed" in formatted_row.lower() or "car" in formatted_row.lower() or "0" == formatted_row.lower() or     ('3' in formatted_row.lower() and 'wh' in formatted_row.lower()) or "convertible" in formatted_row.lower()    or "delv" in formatted_row.lower() or "mini" in formatted_row.lower() or "pas" in formatted_row.lower()    or "pc" in formatted_row.lower() or ("3" in formatted_row.lower() and "doo" in formatted_row.lower())    or ("2" in formatted_row.lower() and "doo" in formatted_row.lower()) or ("nypd" in formatted_row.lower()    and "tow" not in formatted_row.lower()) or "station wa" in formatted_row.lower() or "suv" in formatted_row.lower()    or "taxi" in formatted_row.lower() or "wagon" in formatted_row.lower():
        return row + Row(VEHICLE_GROUP = "Light")
    elif "bicycle" in formatted_row.lower() or "bik" in formatted_row.lower() or "mope" in formatted_row.lower()    or "dirt" in formatted_row.lower() or "sco" in formatted_row.lower() or "fork" in formatted_row.lower() or     "dolly" in formatted_row.lower() or "motor" in formatted_row.lower() or     ("mo" in formatted_row.lower() and "p" in formatted_row.lower()) or ("2" in formatted_row.lower() and "wh" in    formatted_row.lower()) or "pallet" in formatted_row.lower() or "pedicab" in formatted_row.lower():
        return row + Row(VEHICLE_GROUP = "Small")
    elif "n/a" in formatted_row.lower() or "na" in formatted_row.lower() or "other" in formatted_row.lower() or    "unk" in formatted_row.lower() or "ukn" in formatted_row.lower():
        return row + Row(VEHICLE_GROUP = "Other")
    else:
        return row + Row(VEHICLE_GROUP = "Unspecified")

vehicle_types_sql = "SELECT COLLISION_ID,VEHICLE_TYPE,VEHICLE_MAKE FROM vData WHERE VEHICLE_TYPE NOT LIKE 'NULL' OR VEHICLE_MAKE NOT LIKE 'NULL' ORDER BY COLLISION_ID"

vehicle_types_df = spark.sql(vehicle_types_sql)

# print("***** Vehicle Group Dataframe ******")

# print(vehicle_types_df.show())

# print("Grouped_vehicles count:", vehicle_types_df.count())

vehicle_type_rdd = vehicle_types_df.rdd.map(vehicle_classifier).filter(lambda x: x is not None)

schema = StructType([
    StructField("COLLISION_ID", IntegerType(), True),
    StructField("VEHICLE_TYPE", StringType(), True),
    StructField("VEHICLE_MAKE", StringType(), True),
    StructField("VEHICLE_GROUP", StringType(), True)
])

vehicle_type_df_final = spark.createDataFrame(vehicle_type_rdd,schema=schema)

vehicle_type_df_final = vehicle_type_df_final.where(vehicle_type_df_final["VEHICLE_GROUP"] != "Unspecified")

vehicle_groups_df = vehicle_type_df_final.groupBy("VEHICLE_GROUP").count().withColumnRenamed("count","ACCIDENTS").orderBy("VEHICLE_GROUP")

# print("***** Vehicle Group with Accidents count Dataframe ******")
# print(vehicle_groups_df.show())

vehicle_type_df_final.createOrReplaceTempView("vcData")

vehicle_collision_type_sql = "SELECT SUM(CASE UPPER(VEHICLE_GROUP) WHEN 'SMALL' THEN 1 ELSE 0 END) as small_count,\
 SUM(CASE UPPER(VEHICLE_GROUP) WHEN 'LIGHT' THEN 1 ELSE 0 END) as light_count,\
 SUM(CASE UPPER(VEHICLE_GROUP) WHEN 'MEDIUM' THEN 1 ELSE 0 END) as medium_count,\
 SUM(CASE UPPER(VEHICLE_GROUP) WHEN 'HEAVY' THEN 1 ELSE 0 END) as heavy_count,\
 SUM(CASE UPPER(VEHICLE_GROUP) WHEN 'OTHER' THEN 1 ELSE 0 END) as other_count,\
 COLLISION_ID\
 FROM vcData\
 GROUP BY COLLISION_ID;"

vehicle_collision_type_df = spark.sql(vehicle_collision_type_sql)


# print("Collision Type dataframe count:",vehicle_collision_type_df.count())

# print("***** Vehicle Collision Type Dataframe ******")

# print(vehicle_collision_type_df.show())


def vehicle_collision_detector(row):
    s = ""
    st = ""
    if row[0]>0:
        s+="S"*row[0]
        st+="S"
    if row[1]>0:
        s+="L"*row[1]
        st+="L"
    if row[2]>0:
        s+="M"*row[2]
        st+="M"
    if row[3]>0:
        s+="H"*row[3]
        st+="H"
    if row[4]>0:
        s+="O"*row[4]
        st+="O"
    return row+Row(ACC_IN_BETWEEN = s)+Row(ACC_IN_BETWEEN_UNQ = st)

vehicle_collision_type_rdd = vehicle_collision_type_df.rdd.map(vehicle_collision_detector)

# for x in vehicle_collision_type_rdd.take(5):
#     print(x)


schema = StructType([
    StructField("small_count", IntegerType(), True),
    StructField("light_count", IntegerType(), True),
    StructField("medium_count", IntegerType(), True),
    StructField("heavy_count", IntegerType(), True),
    StructField("other_count", IntegerType(), True),
    StructField("COLLISION_ID", IntegerType(), True),
    StructField("ACC_IN_BETWEEN", StringType(), True),
    StructField("ACC_IN_BETWEEN_UNQ", StringType(), True)
])
vehicle_collision_type_df_final = spark.createDataFrame(vehicle_collision_type_rdd,schema=schema)


vehicle_collision_type_df_final = vehicle_collision_type_df_final.groupBy("ACC_IN_BETWEEN_UNQ").count().withColumnRenamed("count","GROUPS")
# print("***** Vehicle Collision Type Finalised Dataframe ******")

# print(vehicle_collision_type_df_final.show())

# print(vehicle_collision_type_df_final.count())

vehicle_collision_type_df_final_part1 = vehicle_collision_type_df_final.orderBy("GROUPS").limit(19)
vehicle_collision_type_df_final_part2 = vehicle_collision_type_df_final.orderBy("GROUPS").subtract(vehicle_collision_type_df_final_part1).orderBy("GROUPS").limit(10)

# print("***** Vehicle Collision Type Minor part ******")

# print(vehicle_collision_type_df_final_part1.show())

# print("***** Vehicle Collision Type Major part ******")

# print(vehicle_collision_type_df_final_part2.show())

vehicle_collision_type_df_final_pandas = vehicle_collision_type_df_final.toPandas()

#vehicle_collision_type_df_final_pandas.to_csv("graph1.csv")

# fig_pie = px.pie(vehicle_collision_type_df_final_pandas, names="ACC_IN_BETWEEN_UNQ", values="GROUPS",color_discrete_sequence=px.colors.sequential.RdBu)
# fig_pie.update_layout(height=800, width=800)
# #fig_pie.show()


vehicle_collision_type_df_final_part1_pandas = vehicle_collision_type_df_final_part1.toPandas()


vehicle_collision_type_df_final_part2_pandas = vehicle_collision_type_df_final_part2.toPandas()

#vehicle_collision_type_df_final_part1_pandas.to_csv("graph1_1.csv")
#vehicle_collision_type_df_final_part2_pandas.to_csv("graph1_2.csv")

# fig_pie_1 = px.pie(vehicle_collision_type_df_final_part1_pandas, names="ACC_IN_BETWEEN_UNQ", values="GROUPS",color_discrete_sequence=px.colors.sequential.RdBu)
# fig_pie_1.update_layout(height=800, width=800)
# #fig_pie_1.show()

# fig_pie_2 = px.pie(vehicle_collision_type_df_final_part2_pandas, names="ACC_IN_BETWEEN_UNQ", values="GROUPS",color_discrete_sequence=px.colors.sequential.RdBu)
# fig_pie_2.update_layout(height=800, width=800)
# #fig_pie_2.show()


# vehicle_group_pandas = vehicle_groups_df.toPandas()
# vehicle_group_pandas.to_csv("graph2.csv")

# fig = px.histogram(vehicle_group_pandas, x="VEHICLE_GROUP", y="ACCIDENTS", title="Vehicle Group vs Accidents")


# #fig.show()


yearly_vehicle_sql = "SELECT VEHICLE_YEAR, COUNT(*) AS YEARLY_COUNT FROM vData WHERE VEHICLE_YEAR NOT LIKE 'NULL' AND VEHICLE_YEAR > '1111' AND VEHICLE_YEAR < '2025'GROUP BY VEHICLE_YEAR ORDER BY VEHICLE_YEAR"

yearly_vehicle_df = spark.sql(yearly_vehicle_sql)

# print("****Vehicle Manafucturing Year classification*****")

# print(yearly_vehicle_df.show())

# vehicle_year_pandas = yearly_vehicle_df.toPandas()

# vehicle_year_pandas.to_csv("graph3.csv")

# fig_line = px.line(vehicle_year_pandas, x="VEHICLE_YEAR", y="YEARLY_COUNT", title="Vehicle Year vs Accidents")
# #fig_line.show()

# print("*****Contributing Factor Analysis of Unlicensed Drivers*****")

dl_status_sql = """
    SELECT CONTRIBUTING_FACTOR_1, COUNT(*) AS ACCIDENTS
    FROM (
    SELECT CONTRIBUTING_FACTOR_1, DRIVER_LICENSE_STATUS FROM vData WHERE DRIVER_LICENSE_STATUS LIKE 'Unlicensed'
    )
    WHERE CONTRIBUTING_FACTOR_1 NOT LIKE 'Unspecified'
    GROUP BY CONTRIBUTING_FACTOR_1
    ORDER BY CONTRIBUTING_FACTOR_1;
"""


dl_status_df = spark.sql(dl_status_sql).na.drop()
# #dl_status_df.show(150)


dl_status_pandas = dl_status_df.toPandas()
dl_status_pandas.to_csv("graph4.csv")
# # Plot the data using Plotly Express
# fig_pie_dl = px.pie(dl_status_pandas, names="CONTRIBUTING_FACTOR_1", values="ACCIDENTS", title="Contributing Factors",color_discrete_sequence=px.colors.sequential.RdBu)
# fig_pie_dl.update_layout(height=1000, width=1000)
# #fig_pie_dl.show()

