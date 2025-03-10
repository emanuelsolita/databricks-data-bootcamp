# Databricks notebook source
# MAGIC %md
# MAGIC # Put it all together

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Workflow
# MAGIC
# MAGIC This exercise aims to put our previous exercises together into a workflow to orchestrate the end-to-end data pipeline.
# MAGIC
# MAGIC Navigate to *Workflows* and click *Create job*
# MAGIC ![](./docs/workflow_1.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ![](./docs/create_task.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job Cluster
# MAGIC ![](./docs/job_cluster.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a second task
# MAGIC ![](./docs/create_2nd_task.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schedule
# MAGIC
# MAGIC On the right you can find the schedule section. Here you can add a schedule that fit your use case. 
# MAGIC
# MAGIC Modes:
# MAGIC - **Scheduled**: Time trigger
# MAGIC - **File arrival**: Watches your landing zone and triggers upon new file arrival
# MAGIC - **Continuous**: Real time streaming, a cluster is always running. 
# MAGIC
# MAGIC ![schedule](./docs/workflow_schedule.png)

# COMMAND ----------


