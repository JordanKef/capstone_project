#!/usr/bin/env python
# coding: utf-8

# In[1]:


from google.cloud import bigquery
import pandas as pd
from pandas_gbq import to_gbq
import os


# In[2]:


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Iordanis-Pc\Documents\DAProject_Kefalidis\da-capstone-project-kef-e9b3617000c9.json"


# In[3]:


project_id = 'da-capstone-project-kef'
dataset_id = 'Reporting_DB_CapProj'
table_id = 'rep_revenue_per_customer_and_period'


# In[4]:


client = bigquery.Client(project=project_id)

query = """
CREATE OR REPLACE TABLE `da-capstone-project-kef.Reporting_DB_CapProj.rep_revenue_per_customer_and_period` AS
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    DATE(p.payment_date) as date,
    EXTRACT(YEAR FROM p.payment_date) as year,
    EXTRACT(MONTH FROM p.payment_date) as month,
    SUM(p.amount) as total_revenue
FROM `da-capstone-project-kef.Staging_DB_CapProj.stg_customer` c
JOIN `da-capstone-project-kef.Staging_DB_CapProj.stg_payment` p
    ON c.customer_id = p.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, date, year, month
"""


# In[5]:


client.query(query).result()
print("Table created")


# In[8]:


get_ipython().system('jupyter nbconvert rep_revenue_per_customer_and_period.ipynb --to python')

