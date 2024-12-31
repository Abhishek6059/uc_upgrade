# Databricks notebook source
pip install tqdm

# COMMAND ----------

from tqdm import tqdm
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("owner_name", "", "Owner Name")
dbutils.widgets.text("schema_location", "", "Schema Location")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
owner_name = dbutils.widgets.get("owner_name")
schema_location = dbutils.widgets.get("schema_location")

# COMMAND ----------

print(catalog_name)
print(schema_name)
print(owner_name)
print(schema_location)

# COMMAND ----------

# spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_name} CASCADE")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Schema in Catalog

# COMMAND ----------

if schema_location.strip() == "":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
else:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name} MANAGED LOCATION '{schema_location}'")

# COMMAND ----------

if owner_name.strip() != "":
  spark.sql(f"ALTER SCHEMA {catalog_name}.{schema_name} SET OWNER TO `{owner_name}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Execute Sync Dry Run

# COMMAND ----------

# dry_run_df = spark.sql(f"SYNC SCHEMA {catalog_name}.default AS EXTERNAL FROM hive_metastore.{schema_name} DRY RUN")
dry_run_df = spark.sql(f"SYNC SCHEMA {catalog_name}.{schema_name} AS EXTERNAL FROM hive_metastore.{schema_name} DRY RUN")
display(dry_run_df)

# COMMAND ----------

display(
    dry_run_df.groupBy("status_code").count()
)

# COMMAND ----------

display(
    dry_run_df.groupBy("source_type").count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Execute Sync

# COMMAND ----------

if owner_name.strip() != "":
  sync_df = spark.sql(f"SYNC SCHEMA {catalog_name}.{schema_name} AS EXTERNAL FROM hive_metastore.{schema_name} SET OWNER `{owner_name}`")
else:
  sync_df = spark.sql(f"SYNC SCHEMA {catalog_name}.{schema_name} AS EXTERNAL FROM hive_metastore.{schema_name}")
display(sync_df)

# COMMAND ----------

display(
    sync_df.groupBy("status_code").count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Upgrade views to UC

# COMMAND ----------

view_df = sync_df.filter("status_code = 'VIEWS_NOT_SUPPORTED'")
display(view_df)

# COMMAND ----------

catalog_database_list = [row["databaseName"] for row in spark.sql(f"SHOW DATABASES IN {catalog_name}").collect()]
catalog_database_list[:5]

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Generate View Statements

# COMMAND ----------

import re
view_create_statement_list = []
if len(view_df.collect()) > 0:
    for view in tqdm(view_df.collect(), desc="Progress"):
        schema_name = view["source_schema"]
        view_name = view["source_name"]
        # print(view)
        # view_statement_df = spark.sql(f"SHOW CREATE TABLE hive_metastore.{schema_name}.{view_name}")
        view_statement_df = spark.sql(f"DESCRIBE EXTENDED hive_metastore.{schema_name}.{view_name}")
        # display(view_statement_df)
        view_create_statement = ""
        for row in view_statement_df.collect():
            if row["col_name"] == "View Text":
                view_create_statement = row["data_type"]

        for catalog_database in catalog_database_list:
            pattern = re.compile(rf"\b{re.escape(catalog_database)}.\b", flags=re.IGNORECASE)
            view_create_statement = pattern.sub(f"{catalog_name}.{catalog_database}.", view_create_statement)

            pattern = re.compile(rf"\b`{re.escape(catalog_database)}`.\b", flags=re.IGNORECASE)
            view_create_statement = pattern.sub(f"{catalog_name}.{catalog_database}.", view_create_statement)


            pattern = re.compile(rf"\b{re.escape(catalog_database)} .\b", flags=re.IGNORECASE)
            view_create_statement = pattern.sub(f"{catalog_name}.{catalog_database}.", view_create_statement)

            pattern = re.compile(rf"\bhive_metastore.\b", flags=re.IGNORECASE)
            view_create_statement = pattern.sub(f"", view_create_statement)

        view_create_statement = f"CREATE OR REPLACE VIEW {catalog_name}.{schema_name}.{view_name} AS " + view_create_statement
        view_create_statement_list.append({"schema_name": schema_name, "view_name": view_name, "view_stmt": view_create_statement})
view_create_statement_list[0]

# COMMAND ----------

view_create_statement_df = spark.createDataFrame(view_create_statement_list)
display(view_create_statement_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Execute View Statements

# COMMAND ----------

views_create_res_list = []
# successful_views_list = []
for view_create_stmt in tqdm(view_create_statement_list, desc="Progress"):
    try:
        spark.sql(view_create_stmt["view_stmt"])
        views_create_res_list.append({"schema_name": view_create_stmt["schema_name"] ,"view_name": view_create_stmt["view_name"], "view_stmt": view_create_stmt["view_stmt"], "status": "success", "error": ""})
    except Exception as e:
        views_create_res_list.append({"schema_name": view_create_stmt["schema_name"] ,"view_name": view_create_stmt["view_name"], "view_stmt": view_create_stmt["view_stmt"], "status": "failed", "error": str(e)[:32000]})
views_create_res_list[:1]

# COMMAND ----------

if len(views_create_res_list) > 0:
    views_create_res_df = spark.createDataFrame(views_create_res_list)
    display(views_create_res_df)

# COMMAND ----------

failed_views_df = (
    views_create_res_df.filter(col("status") == "failed")
)
display(failed_views_df)

# COMMAND ----------

success_views_df = (
    views_create_res_df.filter(col("status") == "success")
)
display(success_views_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Generate alter statements to change owner

# COMMAND ----------

view_alter_statement_list = []
if owner_name.strip() != "":
    for view in success_views_df.collect():
        schema_name = view["schema_name"]
        view_name = view["view_name"]
        alter_stmt = f"ALTER TABLE {catalog_name}.{schema_name}.{view_name} SET OWNER TO `{owner_name}`"
        view_alter_statement_list.append({"schema_name": schema_name, "view_name": view_name, "alter_stmt": alter_stmt})
    view_alter_statement_list[0]

# COMMAND ----------

view_alter_statement_df = spark.createDataFrame(view_alter_statement_list)
display(view_alter_statement_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Execute alter statements to change owner

# COMMAND ----------

alter_views_res_list = []
for view_alter_stmt in tqdm(view_alter_statement_list, desc="Progress"):
    try:
        spark.sql(view_alter_stmt["alter_stmt"])
        alter_views_res_list.append({"schema_name": view_alter_stmt["schema_name"], "status": "success", "view_name": view_alter_stmt["view_name"], "view_stmt": view_alter_stmt["alter_stmt"]})
    except Exception as e:
        alter_views_res_list.append({"schema_name": view_alter_stmt["schema_name"], "status": "failed", "view_name": view_alter_stmt["view_name"], "view_stmt": view_alter_stmt["alter_stmt"]})
alter_views_res_list[:1]

# COMMAND ----------

if len(alter_views_res_list) > 0:
    alter_views_res_df = spark.createDataFrame(alter_views_res_list)
    display(alter_views_res_df)

# COMMAND ----------

failed_alter_views_res_df = (
    alter_views_res_df.filter(col("status") == "failed")
)
display(failed_alter_views_res_df)

# COMMAND ----------

success_alter_views_res_df = (
    alter_views_res_df.filter(col("status") == "success")
)
display(success_alter_views_res_df)

# COMMAND ----------

