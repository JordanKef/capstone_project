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

WITH revenue AS (

    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        p.payment_date,
        p.amount
    FROM `da-capstone-project-kef.Staging_DB_CapProj.stg_customer` c
    JOIN `da-capstone-project-kef.Staging_DB_CapProj.stg_payment` p
        ON c.customer_id = p.customer_id

)

, reporting_dates AS (

    SELECT *
    FROM `da-capstone-project-kef.Reporting_DB_CapProj.reporting_periods_table`
    WHERE reporting_period IN ('Day','Month','Year')

)

, revenue_per_customer_period AS (

    SELECT
        customer_id,
        first_name,
        last_name,
        'Day' AS reporting_period,
        DATE_TRUNC(DATE(payment_date), DAY) AS reporting_date,
        ROUND(SUM(amount),2) AS total_revenue
    FROM revenue
    GROUP BY 1,2,3,4,5

    UNION ALL

    SELECT
        customer_id,
        first_name,
        last_name,
        'Month',
        DATE_TRUNC(DATE(payment_date), MONTH),
        ROUND(SUM(amount),2)
    FROM revenue
    GROUP BY 1,2,3,4,5

    UNION ALL

    SELECT
        customer_id,
        first_name,
        last_name,
        'Year',
        DATE_TRUNC(DATE(payment_date), YEAR),
         ROUND(SUM(amount),3)
    FROM revenue
    GROUP BY 1,2,3,4,5

)

, customers AS (

    SELECT DISTINCT
        customer_id,
        first_name,
        last_name
    FROM revenue

)

, final AS (

    SELECT DISTINCT
        c.customer_id,
        c.first_name,
        c.last_name,
        reporting_dates.reporting_period,
        reporting_dates.reporting_date,
        COALESCE(r.total_revenue,0) AS total_revenue

    FROM customers c

    CROSS JOIN reporting_dates

    LEFT JOIN revenue_per_customer_period r
        ON c.customer_id = r.customer_id
        AND reporting_dates.reporting_period = r.reporting_period
        AND reporting_dates.reporting_date = r.reporting_date

)

SELECT *
FROM final

"""


# In[5]:


client.query(query).result()
print("Table created")


# In[6]:


get_ipython().system('jupyter nbconvert rep_revenue_per_customer_and_period.ipynb --to python')

