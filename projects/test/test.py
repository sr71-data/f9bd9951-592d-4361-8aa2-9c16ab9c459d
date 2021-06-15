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
from datetime import datetime, timedelta



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
      FROM "STREAMLIT_PUBLIC"."TEST_DATA"."DUMMY"
      LIMIT 100
  '''

  # pull query into dataframe
  df = pd.read_sql_query(sql, engine)

  # close connection and dispose of engine once done.
  connection.close()
  engine.dispose()

  return df


st.title('TESTING')
df = load_data().copy()
st.write(df)
