# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 5: Dashboards

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objective
# MAGIC Create a Databricks Dashboard to visualize the data from the Gold layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC Dashboards in Databricks allow you to:
# MAGIC - Visualize data from tables/views
# MAGIC - Create interactive reports
# MAGIC - Set up automatic refresh schedules
# MAGIC - Share insights with stakeholders
# MAGIC
# MAGIC Use the Gold layer tables you created in Exercise 3.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Navigate to Dashboards
# MAGIC
# MAGIC 1. Click **Dashboards** in the left sidebar
# MAGIC 2. Click **Create Dashboard**
# MAGIC 3. Name your dashboard: `Retail Analytics`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Add a Dataset
# MAGIC
# MAGIC Select a table or view as your data source:
# MAGIC
# MAGIC Available tables from Gold layer:
# MAGIC - `emanuel_db.gold.dim_customers`
# MAGIC - `emanuel_db.gold.dim_products`
# MAGIC - `emanuel_db.gold.dim_stores`
# MAGIC - `emanuel_db.gold.iot_data`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Create Visualizations
# MAGIC
# MAGIC Create at least 3 different visualization types:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization 1: Transaction Count Over Time
# MAGIC
# MAGIC | Setting | Value |
# MAGIC |---------|-------|
# MAGIC | Chart type | Line |
# MAGIC | X-axis | transaction_date |
# MAGIC | Y-axis | COUNT(*) |
# MAGIC | Data source | `gold.iot_data` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization 2: Sales by Store
# MAGIC
# MAGIC | Setting | Value |
# MAGIC |---------|-------|
# MAGIC | Chart type | Bar |
# MAGIC | X-axis | store_name |
# MAGIC | Y-axis | SUM(transaction_amount) |
# MAGIC | Data source | `gold.iot_data` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization 3: Top Products
# MAGIC
# MAGIC | Setting | Value |
# MAGIC |---------|-------|
# MAGIC | Chart type | Pie/Donut |
# MAGIC | Labels | product_name |
# MAGIC | Values | SUM(transaction_amount) |
# MAGIC | Data source | `gold.iot_data` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization 4: Customer Summary Table
# MAGIC
# MAGIC | Setting | Value |
# MAGIC |---------|-------|
# MAGIC | Chart type | Table |
# MAGIC | Columns | All |
# MAGIC | Data source | `gold.dim_customers` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Add Filters (Optional)
# MAGIC
# MAGIC Make your dashboard interactive by adding filters:
# MAGIC
# MAGIC | Filter Type | Purpose |
# MAGIC |-------------|---------|
# MAGIC | Date range | Filter by transaction date |
# MAGIC | Store selector | Filter by store |
# MAGIC | Product category | Filter by product type |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: Set Refresh Schedule
# MAGIC
# MAGIC Configure automatic data refresh:
# MAGIC
# MAGIC 1. Click **Schedule** on your dashboard
# MAGIC 2. Choose frequency:
# MAGIC    - Hourly
# MAGIC    - Daily
# MAGIC    - Weekly
# MAGIC 3. Select time
# MAGIC 4. Save the schedule

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6: Add Parameters (Optional)
# MAGIC
# MAGIC Use dashboard parameters to make visualizations dynamic:
# MAGIC
# MAGIC 1. Create a parameter (e.g., `date_filter`)
# MAGIC 2. Reference it in your SQL queries
# MAGIC 3. Add a date picker widget for user input

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7: Test the Dashboard
# MAGIC
# MAGIC 1. Click **Refresh** to update data
# MAGIC 2. Test the filters
# MAGIC 3. Verify all visualizations load correctly
# MAGIC 4. Check the schedule is active

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sharing

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 8: Share the Dashboard
# MAGIC
# MAGIC Share with others:
# MAGIC
# MAGIC 1. Click **Share** on your dashboard
# MAGIC 2. Add collaborators by email
# MAGIC 3. Set permissions:
# MAGIC    - Can view
# MAGIC    - Can edit
# MAGIC 4. Optionally enable email subscriptions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completion Checklist
# MAGIC
# MAGIC - [ ] Dashboard created
# MAGIC - [ ] At least 3 visualizations added
# MAGIC - [ ] Data source connected to Gold tables
# MAGIC - [ ] Refresh schedule configured
# MAGIC - [ ] Dashboard tested and working
