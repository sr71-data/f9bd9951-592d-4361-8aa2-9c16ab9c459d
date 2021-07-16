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
## FUNCTIONS
@st.cache(suppress_st_warning = True, show_spinner = False)
def load_data(sql):
  
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


## MAIN
# Historical repurchase rate panel
sql = '''
  SELECT *
  FROM "STREAMLIT_PUBLIC"."CUSTOMER_RETENTION"."CUSTOMER_ORDER"
'''
customer_order = load_data(sql).copy()

# convert to date time, and add a year month column
customer_order['created_at_tz'] = pd.to_datetime(customer_order['created_at_tz'])
customer_order['previous_created_at_tz'] = pd.to_datetime(customer_order['previous_created_at_tz'])
customer_order['year_month'] = customer_order['created_at_tz'].dt.strftime('%Y-%m')

# add filter to sidebar
start_date = st.sidebar.date_input(
      label = 'Start Date',
      value = datetime.date(2019, 1, 1))

end_date = st.sidebar.date_input(
      label = 'End Date')

# applying date filter
customer_order_filtered = customer_order[(customer_order['created_at_tz'].dt.date >= start_date)
  & (customer_order['created_at_tz'].dt.date <= end_date)]

# get overall value
total_orders = customer_order_filtered['order_id'].nunique()
repeat_orders = customer_order_filtered[customer_order_filtered['is_repurchase'] == True]['order_id'] \
  .nunique()

repeat_orders_with_mattress = customer_order_filtered[(customer_order_filtered['is_repurchase'] == True)
  & (customer_order_filtered['has_mattress'] == True)]['order_id'] \
    .nunique()

repeat_orders_with_accessory = customer_order_filtered[(customer_order_filtered['is_repurchase'] == True)
  & (customer_order_filtered['has_accessory'] == True)]['order_id'] \
    .nunique()

# historical repurchase percent output
st.write('Total orders: ', total_orders)
st.write('Repeats: ', np.round((repeat_orders/total_orders) * 100, 2), '%')
st.write('Repeats (mattress): ', np.round((repeat_orders_with_mattress/total_orders) * 100, 2), '%')
st.write('Repeats (accessory): ', np.round((repeat_orders_with_accessory/total_orders) * 100, 2), '%')



# visualize
st.subheader('eda')
st.write(customer_order_filtered.head())
st.write('start_date', start_date)
st.write('end_date', end_date)
st.write('total order', total_orders)
st.write('repeat orders', repeat_orders)
st.write('repeat with mattress', repeat_orders_with_mattress)
st.write('repeat with accessory', repeat_orders_with_accessory)









