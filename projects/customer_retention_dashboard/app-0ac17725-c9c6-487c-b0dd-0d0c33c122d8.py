from plotly.tools import make_subplots
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
import datetime
import math


## set app page config
# st.set_page_config(page_title = 'customer retention',
#   layout = 'wide')

## data connection credentials
# Loading Env Variable for logging into snowflake
ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
USERNAME = os.getenv('SNOWFLAKE_USERNAME')
PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
WAREHOUSE = 'STREAMLIT_PUBLIC_WH'
ROLE = 'STREAMLIT_PUBLIC_ROLE'

st.write('debug', USERNAME)
st.write('debug', PASSWORD)
st.write('debug', WAREHOUSE)

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

def removeShortTermRepurchaseFilter(customer_order):
  to_remove = st.sidebar.radio(label = 'Remove short term repurchase? (<2week)',
    options = [False, True],
    key = 'remove_short_term_key')

  if to_remove == True:
    customer_order.loc[customer_order['week_delay'] < 2, 'is_repurchase'] = False

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

  fig.update_xaxes(title_text='Year and month')
  fig.update_yaxes(title_text='% repurchases (repeat / total order)')
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

def productFilter(repurchases):
  product_selection = st.sidebar.selectbox(label = 'Which product to include?',
                      options = ['all', 'mattress', 'accessory'],
                      key = 'section-2_select_box')

  if product_selection == 'mattress': 
    repurchases = repurchases[repurchases['has_mattress'] == True]
  elif product_selection == 'accessory':
    repurchases = repurchases[repurchases['has_accessory'] == True]

  return repurchases

def includedDatasetFilter():
  included_dataset = st.sidebar.multiselect(label = 'Which data set to include?', 
    options = ['Baseline', 'Lightning', 'Selected Month'],
    default = ['Baseline', 'Lightning', 'Selected Month'],
    key = 'section3-multi-select')
  return included_dataset

def monthSelectorFilter(repurchases):
  month_selector = st.sidebar.selectbox(label = 'select year-month',
    options = repurchases['year_month'].sort_values(ascending = False).unique().tolist(),
    key = 'section-2_year_month_selector')
  
  selected_month = repurchases[repurchases['year_month'] == month_selector]
  return selected_month

def purchaseDelayDistributionComponent(repurchases, selected_month, included_dataset):
  baseline = repurchases[repurchases['year_month'].isin(['2021-01', 
                                                          '2021-02', 
                                                          '2021-03', 
                                                          '2021-04', 
                                                          '2021-05'])]                     

  lightning = repurchases[repurchases['year_month'] == '2021-06']

  # Create distribution plot
  fig = go.Figure()

  if 'Baseline' in included_dataset:
    bin_width = 10
    nbinsx = math.ceil((baseline['week_delay'].max() - baseline['week_delay'].min()) / bin_width)
    
    fig.add_trace(go.Histogram(x = baseline['week_delay'], 
                                histnorm='percent',
                                marker = {'color': '#2ab7ca'},
                                nbinsx = nbinsx,
                                name = 'Baseline (Jan - May, 21)'))

  if 'Lightning' in included_dataset:     
    bin_width = 10
    nbinsx = math.ceil((lightning['week_delay'].max() - lightning['week_delay'].min()) / bin_width)                         
    fig.add_trace(go.Histogram(x = lightning['week_delay'], 
                                histnorm='percent',
                                marker = {'color': '#fed766'},
                                nbinsx = nbinsx,
                                name = 'Lightning Sale (June, 21)'))

  if 'Selected Month' in included_dataset:       
    bin_width = 10
    nbinsx = math.ceil((selected_month['week_delay'].max() - selected_month['week_delay'].min()) / bin_width)

    fig.add_trace(go.Histogram(x = selected_month['week_delay'], 
                                histnorm='percent',
                                marker = {'color': '#fe4a49'},
                                nbinsx = nbinsx,
                                name = 'Selected month'))

  fig.update_layout(barmode='overlay')
  fig.update_traces(opacity=0.6)
  fig.update_xaxes(title_text='Delay in weeks')
  fig.update_yaxes(title_text='% of all repurchases')
  st.plotly_chart(fig)

  # st.write('debug', selected_month)
  
def section3DateFilter(customer_order):
  start_date = st.sidebar.date_input(
          label = 'Start Date',
          value = datetime.date(2019, 1, 1),
          key = 'section-3_start_date')

  end_date = st.sidebar.date_input(
        label = 'End Date',
        key = 'section-3_date_end')

  # applying date filter
  customer_order_filtered = customer_order[(customer_order['created_at_tz'].dt.date >= start_date)
    & (customer_order['created_at_tz'].dt.date <= end_date)]
  
  return customer_order_filtered

def section3FilterProduct(customer_order_filtered):
  product_selection = st.sidebar.selectbox(label = 'Which product to include?',
                        options = ['all', 'mattress', 'accessory'],
                        key = 'section-3_select_box')

  if product_selection == 'mattress': 
    customer_order_filtered = customer_order_filtered[customer_order_filtered['has_mattress'] == True]
  elif product_selection == 'accessory':
    customer_order_filtered = customer_order_filtered[customer_order_filtered['has_accessory'] == True]

  return customer_order_filtered

def nthOrderComponent(customer_order_filtered):
  purchase_sequence = customer_order_filtered[['purchase_sequence', 'order_id']].groupby('purchase_sequence') \
    .nunique() \
      .reset_index() \
        .rename(columns = {'purchase_sequence': 'nth order',
          'order_id': 'order_count'})

  total_orders = customer_order_filtered['order_id'].nunique()

  purchase_sequence['% of all orders'] = np.round((purchase_sequence['order_count'] / total_orders) * 100, 2)
  purchase_sequence = purchase_sequence[purchase_sequence['nth order'] > 1]



  fig = px.bar(purchase_sequence,
    x = 'nth order',
    y = '% of all orders')

  st.plotly_chart(fig)
  st.write('All orders: ', total_orders)

## MAIN
st.title('Customer Retention Dashboard')
customer_order = loadCustomerOrder().copy()

# Global Filters ####################################################
st.sidebar.header('Global Filter')
# note: this filter just turns off the is_purchase boolean for short term repurchase. 
# does not delete the row of data. Therefore total order count will still be accurate.
customer_order = removeShortTermRepurchaseFilter(customer_order) 

# SECTION 1 #########################################################
st.sidebar.header('Section 1 - Filters')
customer_order_filtered = dateFilterComponent(customer_order)

st.title('Section 1')
st.subheader('How many % of orders every month are from repeat purchases?')
st.warning('Note: Metrics here are displayed as a % of the total sales (denominator).')
monthlyRepurchaseRateComponent(customer_order_filtered)
overallRepurchaseRateComponent(customer_order_filtered)

# SECTION 2 #########################################################
st.title('Section 2')
st.subheader('What is the usual time delay between the repeat purchases?')
st.warning('Note: Metric here are displayed as a % of the count of REPEAT orders (denominator).')
repurchases = customer_order[customer_order['is_repurchase'] == True]

# Filters
st.sidebar.header('Section 2 - Filters')
repurchases = productFilter(repurchases)
included_dataset = includedDatasetFilter()
selected_month = monthSelectorFilter(repurchases)

# Distribution component
purchaseDelayDistributionComponent(repurchases, selected_month, included_dataset)

# SECTION 3 #########################################################
st.title('Section 3')
st.subheader('How often do repeat purchases gets to the Nth purchase?')
st.warning('''Note: All order here refers to all orders that contains the relevant product type. 
  Denominator can change depending on the product filter used. 
''')

st.sidebar.header('Section 3 - Filters')

# filters
customer_order_filtered = section3DateFilter(customer_order)
customer_order_filtered = section3FilterProduct(customer_order_filtered)

nthOrderComponent(customer_order_filtered)




