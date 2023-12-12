import streamlit as st
from pyspark.sql import SparkSession, Row
from pyspark import SparkConf, SparkContext
import os
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import plotly.express as px
import sys







import Spark_Vehicle_Analysis

fig_pie = Spark_Vehicle_Analysis.fig_1()

# fig_pie = px.pie(vehicle_collision_type_df_final_pandas, names="ACC_IN_BETWEEN_UNQ", values="GROUPS", title="Collisions",color_discrete_sequence=px.colors.sequential.RdBu)
# fig_pie.update_layout(height=800, width=800)
fig_pie.show()