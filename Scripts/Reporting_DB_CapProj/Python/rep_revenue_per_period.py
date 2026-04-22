#!/usr/bin/env python
# coding: utf-8

# In[1]:


from google.cloud import bigquery
import os


# In[2]:


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Iordanis-Pc\Documents\DAProject_Kefalidis\da-capstone-project-kef-e9b3617000c9.json"


# In[3]:


project_id = 'da-capstone-project-kef'
dataset_id = 'Reporting_DB_CapProj'
table_id = 'rep_revenue_per_period'


# In[4]:


client = bigquery.Client(project=project_id)

query = """
CREATE OR REPLACE TABLE `da-capstone-project-kef.Reporting_DB_CapProj.rep_revenue_per_period` AS
SELECT
    DATE(payment_date) as date,
    EXTRACT(YEAR FROM payment_date) as year,
    EXTRACT(MONTH FROM payment_date) as month,
    FORMAT_DATE('%A', DATE(payment_date)) as weekday,
    SUM(amount) as total_revenue
FROM `da-capstone-project-kef.Staging_DB_CapProj.stg_payment`
GROUP BY date, year, month, weekday
"""


# In[ ]:


client.query(query).result()
print("Table created")


# In[8]:


get_ipython().system('jupyter nbconvert rep_revenue_per_period.ipynb --to python')

