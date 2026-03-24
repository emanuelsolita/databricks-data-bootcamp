# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 4: Orchestration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objective
# MAGIC Combine all previous exercises into a scheduled Databricks Workflow that orchestrates the end-to-end data pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC In this exercise, you'll create a **Workflow** (job) that:
# MAGIC 1. Runs each exercise as a separate task
# MAGIC 2. Defines dependencies between tasks
# MAGIC 3. Sets up scheduling
# MAGIC
# MAGIC The workflow should run:
# MAGIC ```
# MAGIC Exercise 1 (Bronze) → Exercise 2 (Silver) → Exercise 3 (Gold)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Create a Workflow
# MAGIC
# MAGIC Navigate to **Workflows** in the left sidebar and click **Create job**.

# COMMAND ----------

# MAGIC %md
# MAGIC ![workflow creation](./docs/workflow_1.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Steps to Create:
# MAGIC
# MAGIC 1. Click **Create job**
# MAGIC 2. Name your job: `retail_data_pipeline`
# MAGIC 3. Add Task 1:
# MAGIC    - Task name: `bronze_layer`
# MAGIC    - Type: Notebook
# MAGIC    - Path: Select `Exercise 1`
# MAGIC 4. Add Task 2:
# MAGIC    - Task name: `silver_layer`
# MAGIC    - Type: Notebook
# MAGIC    - Path: Select `Exercise 2`
# MAGIC    - Depends on: `bronze_layer`
# MAGIC 5. Add Task 3:
# MAGIC    - Task name: `gold_layer`
# MAGIC    - Type: Notebook
# MAGIC    - Path: Select `Exercise 3`
# MAGIC    - Depends on: `silver_layer`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Configure Job Settings
# MAGIC
# MAGIC Set up the job cluster and timeout:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Job Cluster
# MAGIC
# MAGIC Create a dedicated cluster for the workflow:
# MAGIC
# MAGIC | Setting | Value |
# MAGIC |---------|-------|
# MAGIC | Cluster mode | Job cluster (isolated) |
# MAGIC | Runtime | Choose latest |
# MAGIC | Node type | Default |
# MAGIC
# MAGIC ![job cluster config](./docs/job_cluster.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alternative: Existing Cluster
# MAGIC
# MAGIC You can also use an existing cluster:
# MAGIC 1. Select **Existing cluster**
# MAGIC 2. Choose your personal compute cluster

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Add Task Dependencies
# MAGIC
# MAGIC Define the execution order:
# MAGIC
# MAGIC | Task | Depends On |
# MAGIC |------|-----------|
# MAGIC | bronze_layer | (none - runs first) |
# MAGIC | silver_layer | bronze_layer |
# MAGIC | gold_layer | silver_layer |
# MAGIC
# MAGIC In the UI:
# MAGIC 1. Add `silver_layer` task
# MAGIC 2. Set Depends on: `bronze_layer`
# MAGIC 3. Add `gold_layer` task
# MAGIC 4. Set Depends on: `silver_layer`

# COMMAND ----------

# MAGIC %md
# MAGIC ![task dependencies](./docs/create_2nd_task.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Configure Scheduling
# MAGIC
# MAGIC Set up when the workflow should run:
# MAGIC
# MAGIC | Mode | Description |
# MAGIC |------|-------------|
# MAGIC | **Scheduled** | Time-based trigger (cron) |
# MAGIC | **File arrival** | Trigger on new files in path |
# MAGIC | **Continuous** | Real-time streaming (always on) |
# MAGIC
# MAGIC Recommended for this exercise: **Triggered** (manual) or **Scheduled** (daily)

# COMMAND ----------

# MAGIC %md
# MAGIC ### For Scheduled Mode:
# MAGIC
# MAGIC ```
# MAGIC Cron schedule example: 0 0 8 * * ? (daily at 8 AM)
# MAGIC ```
# MAGIC
# MAGIC Or use the visual scheduler to set:
# MAGIC - Frequency: Daily
# MAGIC - Time: 08:00 UTC
# MAGIC
# MAGIC ![schedule config](./docs/workflow_schedule.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: Run the Workflow
# MAGIC
# MAGIC After creating the workflow:
# MAGIC
# MAGIC 1. Click **Run now** to test
# MAGIC 2. Monitor the run in the **Runs** tab
# MAGIC 3. Check each task's output
# MAGIC 4. Fix any errors and re-run
# MAGIC
# MAGIC Monitor for:
# MAGIC - Task status (Success/Failed)
# MAGIC - Duration
# MAGIC - Any error messages

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6: Add Parameters (Optional)
# MAGIC
# MAGIC Pass variables between tasks:
# MAGIC
# MAGIC 1. In the first task, use `dbutils.jobs` to set output values
# MAGIC 2. In dependent tasks, use `spark.conf.get("spark.job.output.key")`
# MAGIC
# MAGIC Example:
# MAGIC ```python
# MAGIC # Task 1
# MAGIC spark.conf.set("job.output.date", "2024-01-01")
# MAGIC
# MAGIC # Task 2
# MAGIC date = spark.conf.get("job.output.date")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7: Verify Pipeline Execution
# MAGIC
# MAGIC After the workflow runs successfully:
# MAGIC
# MAGIC 1. Check all tables exist in each layer
# MAGIC 2. Verify data counts are consistent
# MAGIC 3. Check for any failures in the run history

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Verify all tables exist
# MAGIC SHOW TABLES IN emanuel_db.bronze;
# MAGIC SHOW TABLES IN emanuel_db.silver;
# MAGIC SHOW TABLES IN emanuel_db.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Verify record counts
# MAGIC SELECT 'bronze' as layer, COUNT(*) as total_records FROM emanuel_db.bronze.iot_stream
# MAGIC UNION ALL SELECT 'silver', COUNT(*) FROM emanuel_db.silver.iot_stream
# MAGIC UNION ALL SELECT 'gold', COUNT(*) FROM emanuel_db.gold.iot_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completion Checklist
# MAGIC
# MAGIC - [ ] Workflow created
# MAGIC - [ ] Three tasks added (bronze, silver, gold)
# MAGIC - [ ] Task dependencies configured
# MAGIC - [ ] Job cluster set up
# MAGIC - [ ] Schedule configured (or manual run)
# MAGIC - [ ] Workflow executed successfully
# MAGIC - [ ] All tables verified
