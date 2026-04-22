#!/usr/bin/env python
# coding: utf-8

# In[2]:


from google.cloud import bigquery
import pandas as pd
from pandas_gbq import to_gbq
from sqlalchemy import create_engine
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Iordanis-Pc\Documents\DAProject_Kefalidis\da-capstone-project-kef-e9b3617000c9.json"

project_id = 'da-capstone-project-kef'
dataset_id = 'Staging_DB_CapProj'

engine = create_engine("postgresql://postgres:112358@localhost:5432/pagila")

query = "SELECT * FROM customer"   
df = pd.read_sql(query, engine)

to_gbq(
    df,
    "Staging_DB_CapProj.stg_customer",  
    project_id=project_id,
    if_exists="replace"
)


# In[6]:


get_ipython().system('jupyter nbconvert stg_customer.ipynb --to python')

