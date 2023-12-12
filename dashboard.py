import streamlit as st

st.set_page_config(page_title = "Exploratory Data Analysis", page_icon = ":bar_chart:",layout = "wide")

st.title(":bar_chart: EDA")
st.markdown('<style>div: block-container{padding-top:1rem;}</style>',unsafe_allow_html = True)

st.sidebar.header("Choose your filter:")

borough = st.sidebar.multiselect("Pick a Borough", ["Brooklyn","Bronx","Manhattan","Queens","Staten Island"])

import Spark_Vehicle_Analysis
