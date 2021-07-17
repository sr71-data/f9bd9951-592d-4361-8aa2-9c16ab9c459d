import streamlit as st
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import os
from sqlalchemy.dialects import registry
registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
import datetime
import seaborn as sns
import math
import matplotlib.pyplot as plt
from scipy import stats

## set app page config
st.set_page_config(page_title = 'customer retention',
  layout = 'wide')

## data connection credentials
# Loading Env Variable for logging into snowflake
ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
USERNAME = os.getenv('SNOWFLAKE_USERNAME')
PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
WAREHOUSE = 'PROD_WH'
ROLE = 'PROD_ROLE'

## Utility Functions
@st.cache(suppress_st_warning = True, show_spinner = False)
def loadData(sql):
  
  st.warning('No cache found! First data')

  # creating the sqlalchemy engine
  engine = create_engine(URL(
      account = ACCOUNT,
      user = USERNAME,
      password = PASSWORD,
      warehouse = WAREHOUSE,
      role=ROLE,
  ))

  # Data Loading
  connection = engine.connect()

  # pull query into dataframe
  df = pd.read_sql_query(sql, engine)

  # close connection and dispose of engine once done.
  connection.close()
  engine.dispose()

  return df

## Core Functions
@st.cache(suppress_st_warning = True, show_spinner = False)
def loadCustomerOrder():
  sql = '''
    SELECT *
    FROM "STREAMLIT_PUBLIC"."CUSTOMER_RETENTION"."CUSTOMER_ORDER"
  '''
  customer_order = loadData(sql).copy()

  # convert to date time, and add a year month column
  customer_order['created_at_tz'] = pd.to_datetime(customer_order['created_at_tz'])
  customer_order['previous_created_at_tz'] = pd.to_datetime(customer_order['previous_created_at_tz'])
  customer_order['year_month'] = customer_order['created_at_tz'].dt.strftime('%Y-%m')
  
  return customer_order

def dateFilterComponent(customer_order):
  start_date = st.sidebar.date_input(
        label = 'Start Date',
        value = datetime.date(2019, 1, 1))

  end_date = st.sidebar.date_input(
        label = 'End Date')

  # applying date filter
  customer_order_filtered = customer_order[(customer_order['created_at_tz'].dt.date >= start_date)
    & (customer_order['created_at_tz'].dt.date <= end_date)]

  return customer_order_filtered

def monthlyRepurchaseRateComponent(customer_order_filtered):
  #######
  # monthly graph
  monthly_orders = customer_order_filtered[['year_month', 'order_id']]\
    .groupby('year_month') \
      .nunique() \
        .reset_index() \
          .rename(columns = {'order_id': 'order_count'})

  monthly_repeats = customer_order_filtered[customer_order_filtered['is_repurchase'] == True][['year_month', 'order_id']] \
    .groupby('year_month') \
      .nunique() \
        .reset_index() \
          .rename(columns = {'order_id': 'repurchase_count'})

  monthly_mattress_repeats = customer_order_filtered[(customer_order_filtered['is_repurchase'] == True)
    & (customer_order_filtered['has_mattress'] == True)][['year_month', 'order_id']] \
      .groupby('year_month') \
        .nunique() \
          .reset_index() \
            .rename(columns = {'order_id': 'mattress_repurchase_count'})

  monthly_accessory_repeats = customer_order_filtered[(customer_order_filtered['is_repurchase'] == True)
    & (customer_order_filtered['has_accessory'] == True)][['year_month', 'order_id']] \
      .groupby('year_month') \
        .nunique() \
          .reset_index() \
            .rename(columns = {'order_id': 'accessory_repurchase_count'})      

  # merging all aggregated tables
  monthly_repurchase_summary = monthly_orders.merge(monthly_repeats, how = 'outer', on = 'year_month')
  monthly_repurchase_summary = monthly_repurchase_summary.merge(monthly_mattress_repeats, how = 'outer', on = 'year_month')       
  monthly_repurchase_summary = monthly_repurchase_summary.merge(monthly_accessory_repeats, how = 'outer', on = 'year_month')            
            
  # calculating percentages
  monthly_repurchase_summary['all repurchase %'] = np.round((monthly_repurchase_summary['repurchase_count'] / monthly_repurchase_summary['order_count'])*100, 2)
  monthly_repurchase_summary['mattress repurchase %'] = np.round((monthly_repurchase_summary['mattress_repurchase_count'] / monthly_repurchase_summary['order_count'])*100, 2)
  monthly_repurchase_summary['accessorry repurchase %'] = np.round((monthly_repurchase_summary['accessory_repurchase_count'] / monthly_repurchase_summary['order_count'])*100, 2)

  # re-order the table by date
  monthly_repurchase_summary.sort_values('year_month', ascending = True, inplace = True)

  # plotly plot
  fig = px.line(monthly_repurchase_summary, 
    x = 'year_month', 
    y = ['all repurchase %', 'mattress repurchase %', 'accessorry repurchase %'])

  st.plotly_chart(fig)

def overallRepurchaseRateComponent(customer_order_filtered):
  total_orders = customer_order_filtered['order_id'].nunique()
  repeat_orders = customer_order_filtered[customer_order_filtered['is_repurchase'] == True]['order_id'] \
    .nunique()

  repeat_orders_with_mattress = customer_order_filtered[(customer_order_filtered['is_repurchase'] == True)
    & (customer_order_filtered['has_mattress'] == True)]['order_id'] \
      .nunique()

  repeat_orders_with_accessory = customer_order_filtered[(customer_order_filtered['is_repurchase'] == True)
    & (customer_order_filtered['has_accessory'] == True)]['order_id'] \
      .nunique()

  repeats_percent = np.round((repeat_orders/total_orders) * 100, 2)
  repeats_mattress_percent = np.round((repeat_orders_with_mattress/total_orders) * 100, 2)
  repeats_accessory_percent = np.round((repeat_orders_with_accessory/total_orders) * 100, 2)

  st.write('Total orders: ', total_orders)
  st.write('Repeats: ', repeats_percent, '%')
  st.write('Repeats (mattress): ', repeats_mattress_percent, '%')
  st.write('Repeats (accessory): ', repeats_accessory_percent, '%')

## MAIN
st.title('Customer Retention Dashboard')
customer_order = loadCustomerOrder().copy()

# filters #########################################################
st.sidebar.header('Filters')
st.sidebar.subheader('Date Range')
customer_order_filtered = dateFilterComponent(customer_order)

# Main Components ###################################
st.subheader('How many percent of orders every month are repeat purchases?')
monthlyRepurchaseRateComponent(customer_order_filtered)

st.subheader('What is the overall % of repeat purchase in the time frame selected?')
overallRepurchaseRateComponent(customer_order_filtered)

st.title('under development')

# Repurchase distribution component
# repurchases 


# nth repeat purchase component.









