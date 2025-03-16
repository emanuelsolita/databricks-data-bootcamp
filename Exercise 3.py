# Databricks notebook source
# MAGIC %md
# MAGIC # Aggregation

# COMMAND ----------

# MAGIC %md
# MAGIC Create a consumption ready data views.
# MAGIC
# MAGIC Here we want to create a consolidated view containing transaction data, product data, customer data, and store data.
# MAGIC
# MAGIC Join these table together and create a view or a table.
# MAGIC
# MAGIC Make sure the view does not contain any duplicates based on `transaction_id`. 

# COMMAND ----------

# MAGIC %md
# MAGIC If we choose to create a view instead of a table we can switch the default language to `SQL` in the top left corner.
# MAGIC
# MAGIC ![](./docs/change_language.png)
# MAGIC
# MAGIC
# MAGIC Or directly run SQL in a python notebooks by decorating the top of the cell with:
# MAGIC > `%sql`
# MAGIC
# MAGIC followed by the SQL-query e.g.
# MAGIC ```
# MAGIC %sql
# MAGIC create or replace view <catalog>.<schema>.<table>
# MAGIC as
# MAGIC select 
# MAGIC   *
# MAGIC from ...
# MAGIC ```

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
