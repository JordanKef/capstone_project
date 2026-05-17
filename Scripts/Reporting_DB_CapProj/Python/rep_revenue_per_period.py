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

WITH revenue AS (

    SELECT
        payment_date,
        amount
    FROM `da-capstone-project-kef.Staging_DB_CapProj.stg_payment`

)

, reporting_dates AS (

    SELECT *
    FROM `da-capstone-project-kef.Reporting_DB_CapProj.reporting_periods_table`
    WHERE reporting_period IN ('Day','Month','Year')

)

, revenue_per_period AS (

    SELECT
        'Day' AS reporting_period,
        DATE_TRUNC(DATE(payment_date), DAY) AS reporting_date,
        SUM(amount) AS total_revenue
    FROM revenue
    GROUP BY 1,2

    UNION ALL

    SELECT
        'Month',
        DATE_TRUNC(DATE(payment_date), MONTH),
        SUM(amount)
    FROM revenue
    GROUP BY 1,2

    UNION ALL

    SELECT
        'Year',
        DATE_TRUNC(DATE(payment_date), YEAR),
        SUM(amount)
    FROM revenue
    GROUP BY 1,2

)

, final AS (

    SELECT DISTINCT
    reporting_dates.reporting_period,
    reporting_dates.reporting_date,
    COALESCE(revenue_per_period.total_revenue,0) AS total_revenue

    FROM reporting_dates

    LEFT JOIN revenue_per_period
        ON reporting_dates.reporting_period = revenue_per_period.reporting_period
        AND reporting_dates.reporting_date = revenue_per_period.reporting_date

)

SELECT *
FROM final

"""


# In[5]:


df = client.query(query).to_dataframe()

df.head()


# In[6]:


schema = [
    bigquery.SchemaField('reporting_period', 'STRING'),
    bigquery.SchemaField('reporting_date', 'DATE'),
    bigquery.SchemaField('total_revenue', 'FLOAT'),
]


# In[7]:


full_table_id = f"{project_id}.{dataset_id}.{table_id}"


# In[8]:


def table_exists(client, full_table_id):
    try:
        client.get_table(full_table_id)
        return True
    except Exception:
        return False

if table_exists(client, full_table_id):

    client.delete_table(full_table_id)

    job_config = bigquery.LoadJobConfig(schema=schema)

    job = client.load_table_from_dataframe(
        df,
        full_table_id,
        job_config=job_config
    )

    job.result()

    print(f"Table {full_table_id} overwritten.")


# In[9]:


client.query(query).result()
print("Table created")


# In[10]:


get_ipython().system('jupyter nbconvert rep_revenue_per_period.ipynb --to python')

