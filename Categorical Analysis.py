#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import os
import pandas as pd
import folium
from folium.plugins import HeatMap,MarkerCluster

# os.environ['PYSPARK_DRIVER_PYTHON']='jupyter'
os.environ['PYSPARK_PYTHON']='python'


# In[2]:


import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px


# In[3]:


spark = SparkSession.builder.master("local[*]").config("spark.driver.memory", "4g").appName("sparkan").getOrCreate()


# In[4]:


merged_data = spark.read.format('csv').options(header='true',inferschema='true').load("person_merged_accident.csv")
merged_data.createOrReplaceTempView("merged_data")


# In[5]:


allData = spark.read.format('csv').options(header='true',inferschema='true').load("nonull.csv")
allData.createOrReplaceTempView("allData")


# In[39]:


# population = {'B1' : ["BRONX","BROOKLYN","MANHATTAN","QUEENS","STATEN ISLAND"],
#              'POPULATION' : [1385108,2552911,1585873,2250002,468730]} 
# population_df = pd.DataFrame(population)
# population_df.to_csv("graph_5.csv")

# # PEDESTRIAN 
# 

# In[9]:


# distinct_PED_LOCATION = "SELECT PED_LOCATION,COUNT(*) AS VALS FROM merged_data where  PED_LOCATION is not null"
# distinct_PED_LOCATION += "  AND PED_LOCATION NOT IN ('Unknown','Does Not Apply') GROUP BY PED_LOCATION ORDER BY VALS DESC"
# distinct_PED_ACTION = "SELECT PED_ACTION,COUNT(*) AS VALS FROM merged_data where PED_ACTION is not null"
# distinct_PED_ACTION += " AND PED_ACTION NOT IN ('Unknown','Does Not Apply') GROUP BY PED_ACTION ORDER BY VALS DESC"


# In[10]:


# distinct_PED_ACTION_df = spark.sql(distinct_PED_ACTION)
# distinct_PED_ACTION_df.show()


# In[11]:


# distinct_PED_LOCATION_df = spark.sql(distinct_PED_LOCATION)
# distinct_PED_LOCATION_df.show()


# In[12]:


# pd_action = distinct_PED_ACTION_df.toPandas()
# pd_action.to_csv("graph_6.csv")


# # In[14]:


# fig, ax = plt.subplots(figsize=(15, 9))  

# wedges, texts, autotexts = ax.pie(pd_action['VALS'], labels=None, autopct='%1.1f%%', startangle=90)

# ax.legend(wedges, pd_action['PED_ACTION'], title="Categories", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))

# ax.axis('equal')
# ax.set_title('Pedestrian Action')

# plt.show()


# In[234]:


# borough_PED_ACTION = "SELECT PED_ACTION,B1,COUNT(*) AS VALS FROM merged_data where PED_ACTION is not null"
# borough_PED_ACTION += " AND PED_ACTION NOT IN ('Unknown','Does Not Apply')"
# borough_PED_ACTION += " GROUP BY PED_ACTION,B1 ORDER BY VALS DESC"


# In[ ]:





# In[16]:


# borough_PED_ACTION_df = spark.sql(borough_PED_ACTION)
# # borough_PED_ACTION_df.show()


# # In[17]:


# borough_ped_action = borough_PED_ACTION_df.toPandas()
# # borough_ped_action.to_csv("graph_7.csv")

# In[236]:


# borough_PED_YEAR = "SELECT YEAR(CAST(TO_DATE(CRASH_DATE,'MM/dd/yyyy') as DATE)) as YEAR_OCCUR,COUNT(*) AS VALS FROM merged_data where PED_ACTION is not null"
# borough_PED_YEAR += " AND PED_ACTION  IN ('Crossing With Signal')"
# borough_PED_YEAR += " GROUP BY YEAR_OCCUR  ORDER BY YEAR_OCCUR"


# # In[238]:


# borough_PED_YEAR_df = spark.sql(borough_PED_YEAR)
# borough_PED_YEAR_df.show()


# In[243]:


# PED_ACTION_CF  = """
# SELECT PED_ACTION, CONTRIBUTING_FACTOR_1, COUNT(*) AS VALS FROM merged_data 
# where PED_ACTION = 'Crossing With Signal' AND CONTRIBUTING_FACTOR_1 NOT IN ('Unspecified') and CONTRIBUTING_FACTOR_1 is not null
# GROUP BY CONTRIBUTING_FACTOR_1,PED_ACTION ORDER BY VALS DESC
# """
# PED_ACTION_CF_df = spark.sql(PED_ACTION_CF)
# PED_ACTION_CF_df.show()


# In[248]:


# print(PED_ACTION_CF_df.toPandas()['CONTRIBUTING_FACTOR_1'][0])


# In[249]:


# print(PED_ACTION_CF_df.toPandas()['CONTRIBUTING_FACTOR_1'])


# In[257]:


# PED_ACCIDENT_TIME  = """
# SELECT PED_ACTION,hour(CAST(CRASH_TIME AS TIMESTAMP)) as HOUR_OCCUR, COUNT(*) AS VALS FROM merged_data 
# where PED_ACTION is not null OR PED_ACTION not in ('Unknown','Does Not Apply')
# GROUP BY HOUR_OCCUR,PED_ACTION ORDER BY PED_ACTION,VALS DESC
# """
# PED_ACCIDENT_TIME_df = spark.sql(PED_ACCIDENT_TIME)
# PED_ACCIDENT_TIME_df.show()
#


# In[ ]:





# In[258]:


# pd_accident_time = PED_ACCIDENT_TIME_df.toPandas()
# pd_accident_time.to_csv("graph_8.csv")


# # In[266]:


# PED_ACCIDENT_TIME2  = """
# SELECT hour(CAST(CRASH_TIME AS TIMESTAMP)) as HOUR_OCCUR, COUNT(*) AS VALS FROM merged_data 
# where PED_ACTION is not null
# GROUP BY HOUR_OCCUR ORDER BY HOUR_OCCUR DESC
# """
# PED_ACCIDENT_TIME2_df = spark.sql(PED_ACCIDENT_TIME2)
# # PED_ACCIDENT_TIME2_df.show()


# # In[267]:


# PED_ACCIDENT_TIME2_pd = PED_ACCIDENT_TIME2_df.toPandas()
# PED_ACCIDENT_TIME2_pd.to_csv("graph_9.csv")
# fig = px.line(PED_ACCIDENT_TIME2_pd, x='HOUR_OCCUR', y='VALS', markers=True, line_shape='linear',
#               labels={'VALS': 'Count of Accidents','HOUR_OCCUR':'TIME'}, title='Number of Pedestrian Accidents VS Time of Day')

# fig.show()


# # PEDESTRIAN ACTION IN VARIOUS BOROUGHS VS COUNT

# In[18]:


# sns.set(style="whitegrid")

# plt.figure(figsize=(16, 10))  


# custom_palette = sns.color_palette("husl", n_colors=len(borough_ped_action['B1'].unique()))

# sns.set(style="whitegrid")
# plt.figure(figsize=(12, 8)) 

# sns.barplot(x='PED_ACTION', y='VALS', hue='B1', data=borough_ped_action, palette=custom_palette)

# plt.xlabel('Pedestrian Action')
# plt.ylabel('Number of Accidents')
# plt.title('Number of Accidents by Pedestrian Action and Borough')

# plt.xticks(rotation=45, ha='right')

# plt.legend(title='Borough', bbox_to_anchor=(1.05, 1), loc='upper left')

# plt.show()


# # In[19]:


# getNumOfPedestrianAccidents = "SELECT B1,COUNT(*) As VALS FROM allData WHERE (NUMBER_OF_PEDESTRIANS_INJURED > 0) OR "
# getNumOfPedestrianAccidents += " (NUMBER_OF_PEDESTRIANS_KILLED > 0)  group by B1 ORDER BY VALS"

# getNumOfPedestrianAccidents_df = spark.sql(getNumOfPedestrianAccidents)


# # In[20]:


# getNumOfPedestrianAccidents_df.show()


# # In[31]:


# getNumOfPedestrianAccidents = "SELECT B1,COUNT(*) As VALS FROM allData WHERE (NUMBER_OF_PEDESTRIANS_INJURED > 0) OR "
# getNumOfPedestrianAccidents += " (NUMBER_OF_PEDESTRIANS_KILLED > 0) OR VEHICLE_TYPE_CODE_2 like '%pedestrian%' "
# getNumOfPedestrianAccidents += " OR VEHICLE_TYPE_CODE_1 like '%pedestrian%' OR VEHICLE_TYPE_CODE_3 like '%pedestrian%' group by B1 ORDER BY VALS"

# getNumOfPedestrianAccidents_df = spark.sql(getNumOfPedestrianAccidents)
# getNumOfPedestrianAccidents_df.show()


# # In[46]:


# getNumOfPedestrianAccidents_Yearly = "SELECT YEAR(CAST(TO_DATE(CRASH_DATE,'MM/dd/yyyy') as DATE)) as YEAR_OCCUR,COUNT(*) As VALS "
# getNumOfPedestrianAccidents_Yearly += "FROM allData  WHERE (NUMBER_OF_PEDESTRIANS_INJURED > 0) OR (NUMBER_OF_PEDESTRIANS_KILLED > 0) "
# getNumOfPedestrianAccidents_Yearly += " group by YEAR_OCCUR ORDER BY YEAR_OCCUR"


# # In[47]:


# getNumOfPedestrianAccidents_Yearly_DF = spark.sql(getNumOfPedestrianAccidents_Yearly)
# getNumOfPedestrianAccidents_Yearly_DF.show()


# # In[48]:


# getNumOfPedestrianAccidents_Yearly_pd = getNumOfPedestrianAccidents_Yearly_DF.toPandas()
# getNumOfPedestrianAccidents_Yearly_pd.to_csv("graph_10.csv")
# fig = px.line(getNumOfPedestrianAccidents_Yearly_pd, x='YEAR_OCCUR', y='VALS', markers=True, line_shape='linear',
#               labels={'VALS': 'Count of Accidents','YEAR_OCCUR':'YEAR'}, title='Number of Accidents Per Year')

# fig.show()


# # In[49]:


# getNumOfPedestrianAccidents_Year = "SELECT B1,YEAR(CAST(TO_DATE(CRASH_DATE,'MM/dd/yyyy') as DATE)) as YEAR_OCCUR,COUNT(*) As VALS "
# getNumOfPedestrianAccidents_Year += "FROM allData  WHERE (NUMBER_OF_PEDESTRIANS_INJURED > 0) OR (NUMBER_OF_PEDESTRIANS_KILLED > 0) "
# getNumOfPedestrianAccidents_Year += " group by B1,YEAR_OCCUR ORDER BY B1,YEAR_OCCUR"

# getNumOfPedestrianAccidents_Year_DF = spark.sql(getNumOfPedestrianAccidents_Year)
# getNumOfPedestrianAccidents_Year_DF.show()

# getNumOfPedestrianAccidents_Year_pd = getNumOfPedestrianAccidents_Year_DF.toPandas()
# getNumOfPedestrianAccidents_Year_pd.to_csv("graph11.csv")

# # In[50]:


# sns.set(style="whitegrid")

# plt.figure(figsize=(12, 8))
# sns.barplot(x='YEAR_OCCUR', y='VALS', hue='B1', data=getNumOfPedestrianAccidents_Year_pd)


# plt.title('Pedestrian Accidents for Each Borough and Year')
# plt.xlabel('Borough')
# plt.ylabel('Total Count')

# plt.show()


# # In[70]:


# getCountofPedestrianDeath = " SELECT YEAR(CAST(TO_DATE(CRASH_DATE,'MM/dd/yyyy') as DATE)) as YEAR_OCCUR,"
# getCountofPedestrianDeath += " SUM(NUMBER_OF_PEDESTRIANS_KILLED) AS NUM_DEATHS, SUM(NUMBER_OF_PEDESTRIANS_INJURED) AS NUM_INJURED"
# getCountofPedestrianDeath += " from allData group by YEAR_OCCUR ORDER BY YEAR_OCCUR"

# getCountofPedestrianDeath_df = spark.sql(getCountofPedestrianDeath)
# getCountofPedestrianDeath_df.show()

# getCountofPedestrianDeath_pd = getCountofPedestrianDeath_df.toPandas()
# getCountofPedestrianDeath_pd.to_csv("graph_12.csv")

# # In[58]:


# plt.plot(getCountofPedestrianDeath_pd['YEAR_OCCUR'],getCountofPedestrianDeath_pd['NUM_DEATHS'], marker='o', label='Deaths')


# plt.xlabel('YEAR')
# plt.ylabel('Count')

# plt.title('Number of Deaths vs YEAR')

# plt.legend()
# plt.show()


# plt.plot(getCountofPedestrianDeath_pd['YEAR_OCCUR'],getCountofPedestrianDeath_pd['NUM_INJURED'], marker='s', label='Injuries')

# plt.xlabel('YEAR')
# plt.ylabel('Count')

# plt.title('Number of Injuries vs YEAR')

# plt.legend()
# plt.show()


# # In[218]:


# plt.close()


# # In[ ]:





# # # CYCLIST

# # In[21]:


# # This shows the number of injured or killed Accidents counts involving cyclists
# getNumOfCyclistAccidents = "SELECT B1,COUNT(*) As VALS FROM allData WHERE (NUMBER_OF_CYCLIST_INJURED > 0) OR "
# getNumOfCyclistAccidents += " (NUMBER_OF_CYCLIST_KILLED > 0)  group by B1 ORDER BY VALS"

# getNumOfCyclistAccidents_df = spark.sql(getNumOfCyclistAccidents)
# # getNumOfCyclistAccidents_df.show()


# # # In[22]:


# # # This shows the total number of Accidents counts involving cyclists
# getNumOfCyclistAccidents = "SELECT B1,COUNT(*) As VALS FROM allData WHERE (NUMBER_OF_CYCLIST_INJURED > 0) OR "
# getNumOfCyclistAccidents += " (NUMBER_OF_CYCLIST_KILLED > 0) OR LOWER(VEHICLE_TYPE_CODE_1) like '%bike%' OR LOWER(VEHICLE_TYPE_CODE_2) like '%bike%' "
# getNumOfCyclistAccidents += " OR LOWER(VEHICLE_TYPE_CODE_3) like '%bike%' OR LOWER(VEHICLE_TYPE_CODE_4) like '%bike%' OR LOWER(VEHICLE_TYPE_CODE_5) like '%bike%'"
# getNumOfCyclistAccidents += "  group by B1 ORDER BY VALS"
# getNumOfCyclistAccidents_df = spark.sql(getNumOfCyclistAccidents)
# # getNumOfCyclistAccidents_df.show()


# # # In[35]:


# pd_numcycle = getNumOfCyclistAccidents_df.toPandas()
# pd_numcycle.to_csv("graph_13.csv")

# # # In[36]:


# # fig, ax = plt.subplots(figsize=(12, 9))  # Adjust width and height as needed


# # wedges, texts, autotexts = ax.pie(pd_numcycle['VALS'], labels=None, autopct='%1.1f%%', startangle=90)

# # ax.legend(wedges, pd_numcycle['B1'], title="Categories", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))


# # ax.axis('equal')


# # ax.set_title('Cyclist Accidents')

# # plt.show()


# # # In[41]:


# pd_numcycle_pop =  pd.merge(pd_numcycle, population_df, on='B1', how='inner')
# pd_numcycle_pop['PER_POPULATION'] = (pd_numcycle_pop['POPULATION']/pd_numcycle_pop['POPULATION'].sum()) * 100


# # # In[42]:


# pd_numcycle_pop['PER_ACCIDENT'] = (pd_numcycle_pop['VALS']/pd_numcycle_pop['VALS'].sum()) * 100


# # # In[44]:


# df_melted = pd.melt(pd_numcycle_pop, id_vars='B1', value_vars=['PER_ACCIDENT', 'PER_POPULATION'],
#                     var_name='Metric', value_name='Value')

# df_melted.to_csv("graph_14.csv")

# # sns.set(style="whitegrid")
# # plt.figure(figsize=(10, 6))
# # sns.barplot(x='B1', y='Value', hue='Metric', data=df_melted, palette='muted')

# # plt.xlabel('B1')
# # plt.ylabel('Percentage')
# # plt.title('Population,Accident Percentage VS Borough For Cyclist Accidents')
# # plt.legend(title='Metric', bbox_to_anchor=(1.05, 1), loc='upper left')
# # plt.show()


# # # In[224]:


# getCountofCyclistDeath = " SELECT YEAR(CAST(TO_DATE(CRASH_DATE,'MM/dd/yyyy') as DATE)) as YEAR_OCCUR,"
# getCountofCyclistDeath += " SUM(NUMBER_OF_CYCLIST_KILLED) AS NUM_DEATHS, SUM(NUMBER_OF_CYCLIST_INJURED) AS NUM_INJURED"
# getCountofCyclistDeath += " from allData group by YEAR_OCCUR ORDER BY YEAR_OCCUR"

# getCountofCyclistDeath_df = spark.sql(getCountofCyclistDeath)
# # getCountofCyclistDeath_df.show()


# # # In[225]:


# getCountofCyclistDeath_pd = getCountofCyclistDeath_df.toPandas()
# getCountofCyclistDeath_pd.to_csv("graph15.csv")

# # # In[ ]:





# # # In[226]:



# # plt.plot(getCountofCyclistDeath_pd['YEAR_OCCUR'],getCountofCyclistDeath_pd['NUM_DEATHS'], marker='o', label='Deaths')


# # plt.xlabel('YEAR')
# # plt.ylabel('Count')

# # plt.title('Number of Deaths vs YEAR')

# # plt.legend()
# # plt.show()


# # # In[227]:


# # plt.plot(getCountofCyclistDeath_pd['YEAR_OCCUR'],getCountofCyclistDeath_pd['NUM_INJURED'], marker='o', label='Deaths')


# # plt.xlabel('YEAR')
# # plt.ylabel('Count')

# # plt.title('Number of Injuries vs YEAR')

# # plt.legend()
# # plt.show()


# # # In[270]:


# getNumOfCyclistAccidents_timely = """

# SELECT hour(CAST(CRASH_TIME AS TIMESTAMP)) as HOUR_OCCUR, COUNT(*) AS VALS from allData WHERE
# (NUMBER_OF_CYCLIST_KILLED > 0) OR LOWER(VEHICLE_TYPE_CODE_1) like '%bike%' OR LOWER(VEHICLE_TYPE_CODE_2) like '%bike%'
# OR LOWER(VEHICLE_TYPE_CODE_3) like '%bike%' OR LOWER(VEHICLE_TYPE_CODE_4) like '%bike%' OR LOWER(VEHICLE_TYPE_CODE_5) like '%bike%'
# group by HOUR_OCCUR ORDER BY HOUR_OCCUR

# """
# getNumOfCyclistAccidents_timely_df = spark.sql(getNumOfCyclistAccidents_timely)
# # getNumOfCyclistAccidents_timely_df.show()


# # # In[272]:


# getNumOfCyclistAccidents_timely_pd = getNumOfCyclistAccidents_timely_df.toPandas()
# getNumOfCyclistAccidents_timely_pd.to_csv("graph16.csv")
# fig = px.line(getNumOfCyclistAccidents_timely_pd, x='HOUR_OCCUR', y='VALS', markers=True, line_shape='linear',
#               labels={'VALS': 'Count of Accidents','HOUR_OCCUR':'TIME'}, title='Number of Cyclist Accidents VS Time of Day')

# fig.show()


# # In[279]:


# cyclist_contributing_factor = """

# SELECT CONTRIBUTING_FACTOR_VEHICLE_1, COUNT(*) AS VALS from allData WHERE
# (NUMBER_OF_CYCLIST_KILLED > 0) OR LOWER(VEHICLE_TYPE_CODE_1) like '%bike%' OR LOWER(VEHICLE_TYPE_CODE_2) like '%bike%'
# OR LOWER(VEHICLE_TYPE_CODE_3) like '%bike%' OR LOWER(VEHICLE_TYPE_CODE_4) like '%bike%' OR LOWER(VEHICLE_TYPE_CODE_5) like '%bike%'
# AND CONTRIBUTING_FACTOR_VEHICLE_1 is not null
# group by CONTRIBUTING_FACTOR_VEHICLE_1 ORDER BY VALS DESC

# """
# cyclist_contributing_factor_df = spark.sql(cyclist_contributing_factor)
# cyclist_contributing_factor_df.show()


# # In[ ]:





# # # MOTORIST

# # In[23]:


getNumOfMotoristAccidents = "SELECT B1,COUNT(*) As VALS FROM allData WHERE (NUMBER_OF_MOTORIST_INJURED > 0) OR "
getNumOfMotoristAccidents += " (NUMBER_OF_MOTORIST_KILLED > 0)  "
getNumOfMotoristAccidents += " group by B1 ORDER BY VALS"


# # In[34]:


getNumOfMotoristAccidents_df = spark.sql(getNumOfMotoristAccidents)
# getNumOfMotoristAccidents_df.show()


# # In[37]:


pd_nummotor = getNumOfMotoristAccidents_df.toPandas()
pd_nummotor.to_csv("graph_17.csv")
# # In[38]:


# fig, ax = plt.subplots(figsize=(12, 9))  # Adjust width and height as needed


# wedges, texts, autotexts = ax.pie(pd_nummotor['VALS'], labels=None, autopct='%1.1f%%', startangle=90)

# ax.legend(wedges, pd_nummotor['B1'], title="Categories", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))


# ax.axis('equal')


# ax.set_title('Motorist Accidents')

# plt.show()


# # In[40]:


# pd_nummotor_pop = pd.merge(pd_nummotor, population_df, on='B1', how='inner')
# pd_nummotor_pop['PER_POPULATION'] = (pd_nummotor_pop['POPULATION']/pd_nummotor_pop['POPULATION'].sum()) * 100


# # # In[43]:


# pd_nummotor_pop['PER_ACCIDENT'] = (pd_nummotor_pop['VALS']/pd_nummotor_pop['VALS'].sum()) * 100


# # # In[45]:


# df_melted2 = pd.melt(pd_nummotor_pop, id_vars='B1', value_vars=['PER_ACCIDENT', 'PER_POPULATION'],
#                     var_name='Metric', value_name='Value')

# df_melted2.to_csv("graph_18.csv")

# sns.set(style="whitegrid")
# plt.figure(figsize=(10, 6))
# sns.barplot(x='B1', y='Value', hue='Metric', data=df_melted2, palette='muted')
# plt.xlabel('B1')
# plt.ylabel('Percentage')
# plt.title('Population,Accident Percentage VS Borough For Motorcycle Accidents')
# plt.legend(title='Metric', bbox_to_anchor=(1.05, 1), loc='upper left')
# plt.show()


# # In[51]:


# getNumOfMotoristAccidents_Yearly = "SELECT YEAR(CAST(TO_DATE(CRASH_DATE,'MM/dd/yyyy') as DATE)) as YEAR_OCCUR,COUNT(*) As VALS "
# getNumOfMotoristAccidents_Yearly += "FROM allData  WHERE (NUMBER_OF_MOTORIST_INJURED > 0) OR (NUMBER_OF_MOTORIST_KILLED > 0) "
# getNumOfMotoristAccidents_Yearly += " group by YEAR_OCCUR ORDER BY YEAR_OCCUR"

# getNumOfMotoristAccidents_Yearly_DF = spark.sql(getNumOfMotoristAccidents_Yearly)
# # getNumOfMotoristAccidents_Yearly_DF.show()


# # # In[52]:


# getNumOfMotoristAccidents_Yearly_pd = getNumOfMotoristAccidents_Yearly_DF.toPandas()
# getNumOfMotoristAccidents_Yearly_pd.to_csv("graph_19.csv")

# fig = px.line(getNumOfPedestrianAccidents_Yearly_pd, x='YEAR_OCCUR', y='VALS', markers=True, line_shape='linear',
#               labels={'VALS': 'Count of Accidents','YEAR_OCCUR':'YEAR'}, title='Number of Accidents Per Year')

# fig.show()


# # In[228]:


# getCountofMotoristDeath = " SELECT YEAR(CAST(TO_DATE(CRASH_DATE,'MM/dd/yyyy') as DATE)) as YEAR_OCCUR,"
# getCountofMotoristDeath += " SUM(NUMBER_OF_MOTORIST_KILLED) AS NUM_DEATHS, SUM(NUMBER_OF_MOTORIST_INJURED) AS NUM_INJURED"
# getCountofMotoristDeath += " from allData group by YEAR_OCCUR ORDER BY YEAR_OCCUR"

# getCountofMotoristDeath_df = spark.sql(getCountofMotoristDeath)
# # getCountofMotoristDeath_df.show()

# getCountofMotoristDeath_pd = getCountofMotoristDeath_df.toPandas()
# getCountofMotoristDeath_pd.to_csv("graph_20.csv")

# # In[229]:


# plt.plot(getCountofMotoristDeath_pd['YEAR_OCCUR'],getCountofMotoristDeath_pd['NUM_DEATHS'], marker='o', label='Deaths')


# plt.xlabel('YEAR')
# plt.ylabel('Count')

# plt.title('Number of Deaths vs YEAR')

# plt.legend()
# plt.show()


# # In[232]:


# plt.plot(getCountofMotoristDeath_pd['YEAR_OCCUR'],getCountofMotoristDeath_pd['NUM_INJURED'], marker='s', label='Injuries')

# plt.xlabel('YEAR')
# plt.ylabel('Count')

# plt.title('Number of Injuries vs YEAR')

# plt.legend()
# plt.show()


# # In[ ]:





# # # Weather Data Correlation

# # In[148]:


# weather_data = spark.read.format('csv').options(header='true',inferschema='true').load("Weather_data.csv")
# weather_data.createOrReplaceTempView("weather_data")


# # In[149]:


# weather_data.count


# # In[150]:


# distinct_dates = "SELECT COUNT(DISTINCT time) from weather_data"


# # In[151]:


# distinct_dates_df = spark.sql(distinct_dates)


# # In[152]:


# distinct_dates_df.show()


# # In[199]:


# weather_final = """
# SELECT 
# date_format(TO_DATE(w.time, 'M/d/yyyy'), 'MM/dd/yyyy') AS formatted_date,
# w.*
# FROM weather_data AS w 
# WHERE YEAR(CAST(TO_DATE(w.time,'M/D/yyyy') as DATE)) > 2013
# """


# # In[200]:


# weather_final_df = spark.sql(weather_final)


# # In[201]:


# weather_final_df.show()


# # In[202]:


# weather_final_df.createOrReplaceTempView("final_weather_data")


# # In[203]:


# weather_accident_merged_sql = """
# SELECT ad.*,fw.* from allData ad inner join final_weather_data fw on fw.formatted_date  = ad.CRASH_DATE
# """


# # In[204]:


# weather_accident_merged_df = spark.sql(weather_accident_merged_sql)


# # In[205]:


# weather_accident_merged_df.show()


# # In[206]:


# weather_accident_merged_df.count()


# # In[207]:


# weather_accident_merged_df.createOrReplaceTempView("groupedData")


# # In[208]:


# grouped_data_sql = """
# SELECT COUNT(*) as accident_count,crash_date, Amt_PrecipHourly, Temperature,Visibility

# from groupedData group by crash_date,Amt_PrecipHourly, Temperature,Visibility
# """

# grouped_data = spark.sql(grouped_data_sql)
# grouped_data.show()


# # In[209]:


# grouped_data.count()


# # In[210]:


# grouped_pandas = grouped_data.toPandas()


# # In[211]:


# grouped_pandas = grouped_pandas.drop('crash_date',axis = 1)
# #df.drop('B', axis=1)


# # In[212]:


# grouped_pandas.corr()


# # # Accident Hotspot Analysis

# # In[286]:


# lat_long_sql = "SELECT LATITUDE,LONGITUDE FROM allData where LATITUDE IS NOT NULL and LONGITUDE IS NOT NULL"

# lat_long = spark.sql(lat_long_sql).toPandas()


# # In[2]:


# nyc_map = folium.Map(location=[40.7128, -74.0060], zoom_start=10)


# # In[305]:


# lat_long_sql2 = """
# SELECT LATITUDE,LONGITUDE,COUNT(*) AS VALS FROM allData where  LATITUDE IS NOT NULL and LONGITUDE IS NOT NULL
# AND LATITUDE !=0 AND LONGITUDE !=0 GROUP BY LATITUDE,LONGITUDE HAVING COUNT(*) > 100 ORDER BY VALS DESC
# """
# lat_long2 = spark.sql(lat_long_sql2)


# # In[306]:


# lat_long2.count()


# # In[297]:


# lat_long2.show()


# # In[311]:


# data = lat_long2.toPandas()

# def get_color(count, max_count):
#     color_step = max_count / 100  
#     color_index = int(count / color_step)
#     color_index = min(color_index, 99)
#     return f'#{color_index:02X}0000'  

# max_count = data["VALS"].max()

# m = folium.Map(location=[40.7128, -74.0060], zoom_start=10)

# marker_cluster = MarkerCluster().add_to(m)

# for _, entry in data.iterrows():
#     color = get_color(entry["VALS"], max_count)
#     folium.Marker(
#         location=[entry["LATITUDE"], entry["LONGITUDE"]],
#         popup=f"Accidents: {entry['VALS']}",
#         icon=folium.Icon(color=color),
#     ).add_to(marker_cluster)

# m


# # In[315]:


# data = lat_long2.toPandas()



# max_count = data["VALS"].max()

# m = folium.Map(location=[40.7128, -74.0060], zoom_start=10)
# marker_cluster = MarkerCluster().add_to(m)
# for _, entry in data.iterrows():
#     color = get_color(entry["VALS"], max_count)
#     folium.Marker(
#         location=[entry["LATITUDE"], entry["LONGITUDE"]],
#         popup=f"Accidents: {entry['VALS']}",
#         icon=folium.Icon(color=color),
#     ).add_to(marker_cluster)


# heat_data = [[entry["LATITUDE"], entry["LONGITUDE"], entry["VALS"]] for _, entry in data.iterrows()]
# HeatMap(heat_data).add_to(m)
# m


# # In[ ]:





# # In[ ]:





# # In[ ]:





# # In[ ]:




