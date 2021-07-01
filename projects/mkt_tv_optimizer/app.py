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
import math

## FUNCTIONS

@st.cache(suppress_st_warning = True, show_spinner = False)
def load_data():
  st.write('No cache found! Attempt data load ...')

 # Loading Env Variable for logging into snowflake
  ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
  USERNAME = os.getenv('SNOWFLAKE_USERNAME')
  PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
  WAREHOUSE = 'STREAMLIT_PUBLIC_WH'
  ROLE = 'STREAMLIT_PUBLIC_ROLE'

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

  # write your custom query
  sql = '''
      SELECT * 
      FROM "STREAMLIT_PUBLIC"."MKT_TV"."TV_PROGRAM_OPTIMIZER"
  '''

  # pull query into dataframe
  df = pd.read_sql_query(sql, engine)

  # close connection and dispose of engine once done.
  connection.close()
  engine.dispose()

  return df

@st.cache(suppress_st_warning = True, show_spinner = False)
def basic_filtering(df, remove_outlier = False):
  
  df['ad_time_ntz'] = pd.to_datetime(df['ad_time_ntz'])
  df['month'] = pd.to_datetime(df['month']).dt.date

  # don't want to look at free spots
  df = df[df['cost'] > 0]

  # remove spots that never had any impression
  df = df[df['impression'] > 0]

  # we want the more recent data, based on the past 6 months because the cost and performance have varied greated during covid
  df = df[df['ad_time_ntz'] >= '2021-01-01']

  if remove_outlier == True: 
    # remove extream outliers
    user_stats = df[['timezone', 'users']].groupby(['timezone']).agg(['mean', 'std']).reset_index()
    user_stats.columns = user_stats.columns.map('-'.join)
    user_stats.rename(columns = {'timezone-':'timezone'}, inplace = True)

    df = df.merge(user_stats, on = 'timezone')

    ## remove extreme outliers 5 std from the mean
    df = df[df['users'] <= df['users-mean'] + 5 * df['users-std']]

  return df



## MAIN ###################################
st.title('WEIGHTED: TV program picking optimization')
df = load_data()

st.write('Data Sample')
st.write(df.head(30))


## PRELIMINARY DATA CLEANING, REMOVING OUTLIERS
st.title('Data cleaning')

st.write('First remove all free spots. We want to judge based on paid ads.')
st.write('Remove all 0 impression, as these should not have any users attributed to them')
st.write('Also only filter for this year\'s data')

remove_outlier = st.radio('Remove spots with extreme user counts?', options = [False, True])
df_filtered = basic_filtering(df.copy(), remove_outlier) # cached operation, use copy

# st.write(df_filtered.head())

st.write('specific filtering')
df_specific = df_filtered[(df_filtered['program'] == 'Property Ladder UK')
                      & (df_filtered['timezone'] == 'Australia/Melbourne')]
st.write(df_specific.head())
st.write(df_specific.shape)

st.write('User distribution by states')
state = st.selectbox('Location', df_filtered['timezone'].sort_values(ascending = True) \
  .unique() \
    .tolist())

df_filtered_stated = df_filtered[df_filtered['timezone'] == state]

bin_width= 1
nbins = math.ceil((df_filtered_stated["users"].max() - df_filtered_stated["users"].min()) / bin_width)
users_hist = px.histogram(df_filtered_stated, x = 'users', nbins = nbins)
st.plotly_chart(users_hist)

st.write('Programs with highest user count.')
st.write(df_filtered_stated.sort_values('users', ascending = False).head())

st.write('Calculate mean and standard deviation for users based on each state.')
st.write('We don\'t want to consider programs that have total users that are less than a specific threshold')

# get users mean and standard deviation based on states.
user_stats = df_filtered[['timezone', 'users']].groupby(['timezone']).agg(['mean', 'std']).reset_index()
user_stats.columns = user_stats.columns.map('-'.join)
user_stats.rename(columns = {'timezone-':'timezone'}, inplace = True)
st.write(user_stats)

## Recommendation list
st.title('Filtering out programs that have low AGGREGATED users')

user_threshold = st.number_input('Mininum user threshold: Mean + Threadhold * STD (unit of STD)', min_value = 0)

df_aggregate = df_filtered[['timezone', 
                    'channel',
                    'program',
                    'spot',
                    'cost',
                    'impression',
                    'users']].groupby(['timezone', 'channel', 'program']) \
                      .sum() \
                        .reset_index() \
                          .rename(columns = {'spot': 'total_spots',
                                              'cost': 'total_cost',
                                              'impression': 'total_impression',
                                              'users': 'total_users'})

df_aggregate = df_aggregate.merge(user_stats, on = 'timezone')

## Filter out those where cost and impression is 0
df_aggregate = df_aggregate[(df_aggregate['total_users'] >= df_aggregate['users-mean'] + user_threshold * df_aggregate['users-std']) 
                          & (df_aggregate['total_impression'] > 0)]


## CHECKING OUT DISTRIBUTION AFTER FILTERING HAS COMPLETED
st.write(df_aggregate[['timezone','total_cost']].groupby('timezone').count().rename(columns = {'total_cost': 'count'}))

bin_width= 20
nbins = math.ceil((df_aggregate["total_impression"].max() - df_aggregate["total_impression"].min()) / bin_width)
impression_hist = px.histogram(df_aggregate, x = 'total_impression', nbins = nbins)
st.plotly_chart(impression_hist)

bin_width= 20
# here you can choose your rounding method, I've chosen math.ceil
nbins = math.ceil((df_aggregate["total_users"].max() - df_aggregate["total_users"].min()) / bin_width)
users_hist = px.histogram(df_aggregate, x = 'total_users', nbins = 50)
st.plotly_chart(users_hist)

st.write('summary stats')
st.write(df_aggregate[['total_impression', 'total_users']].describe())
st.write('programs with the most users')
st.write(df_aggregate.sort_values('total_users', ascending = False).head())

## RECOMMENDATION LIST

df_aggregate['cpu'] = df_aggregate['total_cost'] / df_aggregate['total_users']
df_aggregate['upm'] = df_aggregate['total_users'] / df_aggregate['total_impression']

# cost penalty input
df_final = df_aggregate.copy()

state = st.multiselect('Location', df_final['timezone'].sort_values(ascending = True) \
  .unique() \
    .tolist()) 
  
df_final = df_final[df_final['timezone'].isin(state)]

cost_penalty = st.number_input('Cost penalty as CPU^n (unit of n)', min_value = 1, step = 10)


df_final['cpu_weighted'] = np.power(df_final['cpu'], cost_penalty)
# df_final['cpu_weighted'] = df_final['cpu'] * np.power(10, cost_penalty)
df_final['rating'] = df_final['upm']/df_final['cpu_weighted']

# rank by cpu
df_final = df_final.sort_values(by = ['timezone', 'cpu'],
                                    ascending = [True, True]).reset_index(drop = True)


st.write('Most cost effective program: ', state)
st.write(df_final[['timezone', 'channel','program', 'cpu', 'upm', 'total_cost', 'total_impression', 'total_users']])

# rank by rating
df_final = df_final.sort_values(by = ['timezone', 'rating'],
                                    ascending = [True, False]).reset_index(drop = True)

st.write('Most recommended programs in: ', state)
st.write(df_final[['timezone', 'channel','program', 'cpu', 'upm', 'rating', 'total_cost', 'total_impression', 'total_users']])

## plot upm versus cpu (top 20)
trace2 = go.Scatter(
  x = df_final['cpu_weighted'].head(10),
  y = df_final['upm'].head(10),
  mode = 'markers+text',
  text = df_final['program'],
  textposition = 'bottom right'
)

fig2 = go.Figure(data = trace2)

fig2.update_layout(
    title="Top 20 Rated Programs",
    xaxis_title='Expensiveness (CPU weighted)',
    yaxis_title='Freshness (UPM)'
    )

st.plotly_chart(fig2)

# st.write(df_final)
# df_final.to_csv('program_list.csv', index = False)








