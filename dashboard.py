import streamlit as st
from pyspark.sql import SparkSession, Row
from pyspark import SparkConf, SparkContext
import os
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import plotly.express as px
import sys
import seaborn as sns
import matplotlib.pyplot as plt

def main():
    st.set_page_config(page_title = "Exploratory Data Analysis", page_icon = ":bar_chart:",layout = "wide")
    
    st.title(":bar_chart: EDA")
    st.markdown('<style>div: block-container{padding-top:1rem;}</style>',unsafe_allow_html = True)
    
    st.sidebar.header("Choose your filter:")
    borough = st.sidebar.selectbox("Pick a choice", ("Pedestrian","Cyclist","Motorist","Vehicle"))

    if borough == ("Vehicle"):
        vehicle_collision_type_df_final_pandas = pd.read_csv("graph_1.csv")
        st.header("Vehicle Collisions Pie")
        fig_pie = px.pie(vehicle_collision_type_df_final_pandas, names="ACC_IN_BETWEEN_UNQ", values="GROUPS",color_discrete_sequence=px.colors.sequential.RdBu)
        st.plotly_chart(fig_pie)
        if st.toggle("Expand plot for higher resolution"):
            st.subheader("Minor vehicle collisions")
            vehicle_collision_type_df_final_part1_pandas = pd.read_csv("graph_1_1.csv")
            fig_pie_1 = px.pie(vehicle_collision_type_df_final_part1_pandas, names="ACC_IN_BETWEEN_UNQ", values="GROUPS",color_discrete_sequence=px.colors.sequential.RdBu)
            st.plotly_chart(fig_pie_1)
            
            st.subheader("Major Vehicle collisions")
            vehicle_collision_type_df_final_part2_pandas = pd.read_csv("graph_1_2.csv")
            fig_pie_2 = px.pie(vehicle_collision_type_df_final_part2_pandas, names="ACC_IN_BETWEEN_UNQ", values="GROUPS",color_discrete_sequence=px.colors.sequential.RdBu)
            st.plotly_chart(fig_pie_2)
        st.header("Vehicle Groups Bar")
        vehicle_group_pandas = pd.read_csv("graph_2.csv")
        fig = px.histogram(vehicle_group_pandas, x="VEHICLE_GROUP", y="ACCIDENTS")
        st.plotly_chart(fig)

        st.header("Vehicle Years Line")
        vehicle_year_pandas = pd.read_csv("graph_3.csv")
        fig_line = px.line(vehicle_year_pandas, x="VEHICLE_YEAR", y="YEARLY_COUNT")
        st.plotly_chart(fig_line)

        st.header("Unlicensed Drivers Plot")
        dl_status_pandas = pd.read_csv("graph_4.csv")
        fig_pie_dl = px.pie(dl_status_pandas, names="CONTRIBUTING_FACTOR_1", values="ACCIDENTS",color_discrete_sequence=px.colors.sequential.RdBu)
        st.plotly_chart(fig_pie_dl)
    
    if borough == ("Pedestrian"):
        st.header("Pedestrian Analysis")
        pd_action = pd.read_csv("graph_6.csv")
        fig = px.pie(pd_action, values='VALS', names='PED_ACTION', title='Pedestrian Action',
                labels={'VALS': 'Value', 'PED_ACTION': 'Pedestrian Action'},
                template='plotly_dark')
        PED_ACCIDENT_TIME2_pd = pd.read_csv("graph_9.csv")
        fig = px.line(PED_ACCIDENT_TIME2_pd, x='HOUR_OCCUR', y='VALS', markers=True, line_shape='linear',
                  labels={'VALS': 'Count of Accidents','HOUR_OCCUR':'TIME'})
        st.plotly_chart(fig)


        borough_ped_action = pd.read_csv("graph_7.csv")
        sns.set(style="whitegrid")
        st.header('Number of Accidents by Pedestrian Action and Borough')
        custom_palette = sns.color_palette("husl", n_colors=len(borough_ped_action['B1'].unique()))
        fig, ax = plt.subplots(figsize=(12, 8))
        sns.barplot(x='PED_ACTION', y='VALS', hue='B1', data=borough_ped_action, palette=custom_palette, ax=ax)
        ax.set_xlabel('Pedestrian Action')
        ax.set_ylabel('Number of Accidents')
        ax.set_title('Number of Accidents by Pedestrian Action and Borough')
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
        ax.legend(title='Borough', bbox_to_anchor=(1.05, 1), loc='upper left')
        st.pyplot(fig)

        getNumOfPedestrianAccidents_Yearly_pd = pd.read_csv("graph_10.csv")
        st.header("Pedestrian Accidents every Year")
        fig_ped = px.line(getNumOfPedestrianAccidents_Yearly_pd, x='YEAR_OCCUR', y='VALS', markers=True, line_shape='linear',
                labels={'VALS': 'Count of Accidents','YEAR_OCCUR':'YEAR'})
        st.plotly_chart(fig_ped)

        getNumOfPedestrianAccidents_Year_pd = pd.read_csv("graph_11.csv")
        sns.set(style="whitegrid")
        st.title('Pedestrian Accidents for Each Borough and Year')
    
        fig, ax = plt.subplots(figsize=(12, 8))
        sns.barplot(x='YEAR_OCCUR', y='VALS', hue='B1', data=getNumOfPedestrianAccidents_Year_pd, ax=ax)
    
        ax.set_title('Pedestrian Accidents for Each Borough and Year')
        ax.set_xlabel('Year')
        ax.set_ylabel('Total Count')
    
        ax.legend(title='Borough', bbox_to_anchor=(1.05, 1), loc='upper left')
    
        st.pyplot(fig)

        st.header('Number of Deaths and Injuries vs YEAR')
        getCountofPedestrianDeath_pd = pd.read_csv("graph_12.csv")
    
        fig_deaths, ax_deaths = plt.subplots(figsize=(12, 8))
        ax_deaths.plot(getCountofPedestrianDeath_pd['YEAR_OCCUR'], getCountofPedestrianDeath_pd['NUM_DEATHS'], marker='o', label='Deaths')
    
        ax_deaths.set_xlabel('YEAR')
        ax_deaths.set_ylabel('Count')
        ax_deaths.set_title('Number of Deaths vs YEAR')
    
        st.pyplot(fig_deaths)
    
        fig_injuries, ax_injuries = plt.subplots(figsize=(12, 8))
        ax_injuries.plot(getCountofPedestrianDeath_pd['YEAR_OCCUR'], getCountofPedestrianDeath_pd['NUM_INJURED'], marker='s', label='Injuries')
    
        ax_injuries.set_xlabel('YEAR')
        ax_injuries.set_ylabel('Count')
        ax_injuries.set_title('Number of Injuries vs YEAR')
    
        ax_injuries.legend()
    
        st.pyplot(fig_injuries)
    
    if borough ==("Cyclist"):
        st.header('Cyclist Accidents')
        pd_numcycle = pd.read_csv("graph_13.csv")

        fig = px.pie(pd_numcycle, values='VALS', title='Cyclist Accidents',
                 labels={'VALS': 'Accidents'},
                template='plotly_dark')
    
        st.plotly_chart(fig)
    
        df_melted = pd.read_csv("graph_14.csv")
    
        sns.set(style="whitegrid")
        st.header('Population, Accident Percentage VS Borough For Cyclist Accidents')
    
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.barplot(x='B1', y='Value', hue='Metric', data=df_melted, palette='muted', ax=ax)
    
        ax.set_xlabel('B1')
        ax.set_ylabel('Percentage')
        ax.set_title('Population, Accident Percentage VS Borough For Cyclist Accidents')
    
        ax.legend(title='Metric', bbox_to_anchor=(1.05, 1), loc='upper left')
        st.pyplot(fig)

        st.header('Number of Cyclists Deaths and Injuries vs YEAR')
        getCountofCyclistDeath_pd = pd.read_csv("graph_15.csv")
    
        fig_deaths, ax_deaths = plt.subplots(figsize=(12, 8))
        ax_deaths.plot(getCountofCyclistDeath_pd['YEAR_OCCUR'], getCountofCyclistDeath_pd['NUM_DEATHS'], marker='o', label='Deaths')
    
        ax_deaths.set_xlabel('YEAR')
        ax_deaths.set_ylabel('Count')
        ax_deaths.set_title('Number of Deaths vs YEAR')
    
        st.pyplot(fig_deaths)
    
        fig_injuries, ax_injuries = plt.subplots(figsize=(12, 8))
        ax_injuries.plot(getCountofCyclistDeath_pd['YEAR_OCCUR'], getCountofCyclistDeath_pd['NUM_INJURED'], marker='s', label='Injuries')
    
        ax_injuries.set_xlabel('YEAR')
        ax_injuries.set_ylabel('Count')
        ax_injuries.set_title('Number of Injuries vs YEAR')
    
        ax_injuries.legend()
    
        st.pyplot(fig_injuries)

        getNumOfCyclistAccidents_timely_pd = pd.read_csv("graph_16.csv")

        fig = px.line(getNumOfCyclistAccidents_timely_pd, x='HOUR_OCCUR', y='VALS', markers=True, line_shape='linear',
                labels={'VALS': 'Count of Accidents','HOUR_OCCUR':'TIME'})
    
    if borough == ("Motorist"):
        st.header('Motorist Accidents')
    
        pd_nummotor = pd.read_csv("graph_17.csv")

        fig = px.pie(pd_nummotor, values='VALS', title='Motorist Accidents',
                 labels={'VALS': 'Value'},
                 template='plotly_dark')

        st.plotly_chart(fig)

        df_melted2 = pd.read_csv("graph_18.csv")
    
        sns.set(style="whitegrid")
        st.header('Population, Accident Percentage VS Borough For Motorcycle Accidents')
    
        fig, ax = plt.subplots(figsize=(10, 6))
    
        sns.barplot(x='B1', y='Value', hue='Metric', data=df_melted2, palette='muted', ax=ax)
    
        ax.set_xlabel('B1')
        ax.set_ylabel('Percentage')
        ax.set_title('Population, Accident Percentage VS Borough For Motorcycle Accidents')
    
        ax.legend(title='Metric', bbox_to_anchor=(1.05, 1), loc='upper left')
    
        st.pyplot(fig)

        getNumOfMotoristAccidents_Yearly_pd = pd.read_csv("graph_19.csv")

        fig = px.line(getNumOfMotoristAccidents_Yearly_pd, x='YEAR_OCCUR', y='VALS', markers=True, line_shape='linear',
                labels={'VALS': 'Count of Accidents','YEAR_OCCUR':'YEAR'}, title='Number of Accidents Per Year')
    
        st.plotly_chart(fig)

        getCountofMotoristDeath_pd = pd.read_csv("graph_20.csv")
    
        st.header('Number of Deaths and Injuries vs YEAR')
    
        fig_deaths, ax_deaths = plt.subplots(figsize=(12, 8))
        ax_deaths.plot(getCountofMotoristDeath_pd['YEAR_OCCUR'], getCountofMotoristDeath_pd['NUM_DEATHS'], marker='o', label='Deaths')
        ax_deaths.set_xlabel('YEAR')
        ax_deaths.set_ylabel('Count')
        ax_deaths.set_title('Number of Deaths vs YEAR')
    
        ax_deaths.legend()
        st.pyplot(fig_deaths)
    
        fig_injuries, ax_injuries = plt.subplots(figsize=(12, 8))
        ax_injuries.plot(getCountofMotoristDeath_pd['YEAR_OCCUR'], getCountofMotoristDeath_pd['NUM_INJURED'], marker='s', label='Injuries')
    
        ax_injuries.set_xlabel('YEAR')
        ax_injuries.set_ylabel('Count')
        ax_injuries.set_title('Number of Injuries vs YEAR')
        ax_injuries.legend()
    
        st.pyplot(fig_injuries)

if __name__ =='__main__':
    main()