# Databricks notebook source
dbutils.widgets.dropdown("Environment", "dev", ["dev", "test", "prod"], "Set the current environment/catalog name")
env = dbutils.widgets.get("Environment")


# COMMAND ----------

# MAGIC %run ./Setup

# COMMAND ----------

SH = SetupHelper(env)

# COMMAND ----------

setup_required = spark.sql(f"SHOW DATABASES IN {SH.catalog}").filter(f"databaseName in ('{SH.bronze_db}','{SH.silver_db}','{SH.gold_db}')").count() != 3
if setup_required:
    SH.setup()
    SH.validate()

# COMMAND ----------

# MAGIC %run ./Bronze

# COMMAND ----------

# MAGIC %run ./Silver

# COMMAND ----------

# MAGIC %run ./Gold

# COMMAND ----------

BZ = Bronze(env)
SL = Silver(env)
GL = Gold(env)

# COMMAND ----------

BZ.consume()

# COMMAND ----------

SL.upsert()

# COMMAND ----------

GL.upsert()
