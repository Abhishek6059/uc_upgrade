# Databricks notebook source
pip install tdqm

# COMMAND ----------

from tqdm import tqdm
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Row

# COMMAND ----------

dbutils.widgets.text("schema_name", "", "Schema Name")
# dbutils.widgets.text("mnt_schema_location", "", "Mount Schema Location")
dbutils.widgets.text("s3_schema_location", "", "S3 Schema Location")
dbutils.widgets.text("owner_name", "", "Owner Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")

# COMMAND ----------

CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
OWNER_NAME = dbutils.widgets.get("owner_name")
# MNT_SCHEMA_LOCATION = dbutils.widgets.get("mnt_schema_location")
S3_SCHEMA_LOCATION = dbutils.widgets.get("s3_schema_location")
print(CATALOG_NAME)
print(SCHEMA_NAME)
print(OWNER_NAME)
# print(MNT_SCHEMA_LOCATION)
print(S3_SCHEMA_LOCATION)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Table List

# COMMAND ----------

schema_tables_list = [[SCHEMA_NAME, table["tableName"]] for table in spark.sql(f"SHOW TABLES IN hive_metastore.{SCHEMA_NAME}").collect()]
print(len(schema_tables_list))
schema_tables_list[:5]

# COMMAND ----------

schema_tables_schema = "database string, table string"
tables_df = spark.createDataFrame(schema_tables_list, schema=schema_tables_schema)
display(tables_df)

# COMMAND ----------

def get_table_partition_columns(schema_name, table_name):
  desc_df = spark.sql(f"DESCRIBE TABLE hive_metastore.{schema_name}.{table_name}")
  # display(desc_df)
  partition_ind_flag = 0
  partition_column_list = []
  for row in desc_df.collect():
    if partition_ind_flag == 2:
      partition_column_list.append(row["col_name"])
    if row["col_name"] == "# Partition Information":
      partition_ind_flag += 1
    if (row["col_name"] == "# col_name") and (partition_ind_flag == 1):
      partition_ind_flag += 1
  return partition_column_list

# COMMAND ----------

def get_detailed_table_details(schema_name, tbl_name):
  location, provider, owner, tbl_type, view_text, view_original_text = "", "", "", "", "", ""
  partition_column_list = []
  try:
    ext_tbl_df = spark.sql(f"DESCRIBE TABLE EXTENDED hive_metastore.{schema_name}.{tbl_name}")
    for tbl_row in ext_tbl_df.collect():
      if tbl_row["col_name"]=='Location':
        location = tbl_row["data_type"]
      if tbl_row["col_name"]=='Provider':
        provider = tbl_row["data_type"]
      if tbl_row["col_name"]=='Owner':
        owner = tbl_row["data_type"]
      if tbl_row["col_name"]=='Type':
        tbl_type = tbl_row["data_type"]
      if tbl_row["col_name"]=='View Text':
        view_text = tbl_row["data_type"]
      # if tbl_row["col_name"]=='View Original Text':
      #   view_original_text = tbl_row["data_type"]

      partition_column_list = get_table_partition_columns(schema_name, tbl_name)

    if tbl_type == "VIEW":
      return {"Database": schema_name, "Table": tbl_name, "View_Text": view_text, "Owner": owner, "Table_Type": tbl_type}
    else:
      return {"Database": schema_name, "Table": tbl_name, "Location": location, "Provider": provider, "Owner": owner, "Table_Type": tbl_type, "Partition_Columns": partition_column_list}
  except Exception as e:
    return {"Database": schema_name, "Table": tbl_name}
    # print("An error occured while getting table details")

# COMMAND ----------

table_details_list = []
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor() as executor:
  table_details_list = list(tqdm(executor.map(lambda x: get_detailed_table_details(*x), schema_tables_list), total=len(schema_tables_list)))
table_details_list[:1]

# COMMAND ----------

# Define the schema
detailed_tbl_schema = StructType([
    StructField("Database", StringType(), True),
    StructField("Table", StringType(), True),
    StructField("Owner", StringType(), True),
    StructField("Provider", StringType(), True),
    StructField("Table_Type", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("Partition_Columns", ArrayType(StringType()), True)
])

# Create DataFrame with the specified schema
detailed_tbl_df = spark.createDataFrame(table_details_list, schema=detailed_tbl_schema)
display(detailed_tbl_df)

# COMMAND ----------

detailed_tbl_df = (
  detailed_tbl_df.filter((~col("Location").startswith("s3")) | (col("Provider")=="hive"))
)
display(detailed_tbl_df)

# COMMAND ----------

schema_table_list_final = []
for table_row in tqdm(detailed_tbl_df.collect(), desc="Progress"):
    schema_name = table_row["Database"]
    table_name = table_row["Table"]
    curr_loc = table_row["Location"]
    provider = table_row["Provider"]
    partition_col_list = table_row["Partition_Columns"]
    try:
        count_df = spark.sql(f"SELECT COUNT(*) FROM hive_metastore.{schema_name}.{table_name}")
        hive_table_count = count_df.collect()[0]["count(1)"]
        schema_table_list_final.append([schema_name, table_name, curr_loc, provider, partition_col_list, hive_table_count])
    except Exception as e:
        schema_table_list_final.append([schema_name, table_name, curr_loc, provider, partition_col_list, None])

len(schema_table_list_final)

# COMMAND ----------

from pyspark.sql.functions import *
sch_tbl_schema = "database string, table string, curr_loc string, provider string, partition_col_list array<string>, hive_table_count long"
schema_table_final_df = spark.createDataFrame(schema_table_list_final, schema=sch_tbl_schema)
schema_table_final_df = schema_table_final_df.withColumn("ext_loc", concat(lit(S3_SCHEMA_LOCATION), lit("/"), col("table")))
display(schema_table_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Schema

# COMMAND ----------

# if S3_SCHEMA_LOCATION.strip() == "":
#   spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
# else:
#   spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME} MANAGED LOCATION '{S3_SCHEMA_LOCATION}'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")

# COMMAND ----------

if OWNER_NAME.strip() != "":
  spark.sql(f"ALTER SCHEMA {CATALOG_NAME}.{SCHEMA_NAME} SET OWNER TO `{OWNER_NAME}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Copy data files to external location

# COMMAND ----------

def create_uc_table(database, table, curr_loc, ext_loc, provider, partition_col_list):
  try:
    create_table_stmt = ""
    if provider in ['delta', 'iceberg', 'parquet']:
      create_table_stmt = f"CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{database}.{table} DEEP CLONE hive_metastore.{database}.{table} LOCATION '{ext_loc}'" 
    else:
      # create_table_stmt = f"CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{database}.{table} LOCATION '{ext_loc}' AS SELECT * FROM hive_metastore.{database}.{table}"
      create_table_stmt = f"CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{database}.{table} USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name') LOCATION '{ext_loc}' AS SELECT * FROM hive_metastore.{database}.{table}"
      
    # print(create_table_stmt)
    spark.sql(create_table_stmt)
    return {"database": database, "table": table, "curr_loc": curr_loc, "ext_loc":ext_loc, "copied_successfully": True}
  except Exception as e:
    print(e)
    return {"database": database, "table": table, "curr_loc": curr_loc, "ext_loc":ext_loc, "copied_successfully": False}

# COMMAND ----------

copy_tables_list = []
for row in schema_table_final_df.collect():
  database = row["database"]
  table = row["table"]
  curr_loc = row["curr_loc"]
  ext_loc = row["ext_loc"]
  provider = row["provider"]
  partition_col_list = row["partition_col_list"]
  copy_tables_list.append([database, table, curr_loc, ext_loc, provider, partition_col_list])
copy_tables_list[:1]

# COMMAND ----------

copy_table_data_list = []
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor() as executor:
  copy_table_data_list = list(tqdm(executor.map(lambda x: create_uc_table(*x), copy_tables_list), total=len(copy_tables_list)))
copy_table_data_list[:1]

# COMMAND ----------

if len(copy_table_data_list) > 0:
  copy_table_data_df = spark.createDataFrame(copy_table_data_list)
  display(copy_table_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Perform count check

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

validated_schema_table_final_list = []
for row in copy_table_data_df.collect():
    # provider = row["provider"]
    database = row["database"]
    table = row["table"]
    ext_loc_count = None
    try:
        ext_loc_count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG_NAME}.{database}.{table}").collect()[0]["count(1)"]
    except Exception as e:
        ext_loc_count = None
    new_row = Row(
        **row.asDict(), 
        ext_loc_count=ext_loc_count
    )
    validated_schema_table_final_list.append(new_row)

# print(validated_schema_table_final_list)

# Define the schema explicitly
schema = StructType(
    copy_table_data_df.schema.fields + 
    [StructField("uc_table_count", LongType(), True)]
)

validated_schema_table_final_df = spark.createDataFrame(
    validated_schema_table_final_list, 
    schema
)
display(validated_schema_table_final_df)

# COMMAND ----------

final_df = schema_table_final_df.join(validated_schema_table_final_df, ["database", "table"], how="inner")
display(final_df)

# COMMAND ----------

final_true_df = (
  final_df.filter(col("hive_table_count") == col("uc_table_count"))
)
print(final_true_df.count())
display(final_true_df)

# COMMAND ----------

final_false_df = (
  final_df.filter(col("hive_table_count") != col("uc_table_count"))
)
print(final_false_df.count())
display(final_false_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - Alter the owner

# COMMAND ----------

alter_table_owner_list = []
for copy_table_data in tqdm(copy_table_data_list, desc="Progress"):
    database = copy_table_data['database']
    table = copy_table_data['table']
    alter_stmt = f"ALTER TABLE {CATALOG_NAME}.{database}.{table} SET OWNER TO `{OWNER_NAME}`"
    alter_table_owner_list.append(alter_stmt)
alter_table_owner_list[:1]

# COMMAND ----------

if len(alter_table_owner_list) > 0:
    alter_table_owner_df = spark.createDataFrame(alter_table_owner_list, "string")
    display(alter_table_owner_df)

# COMMAND ----------

failed_alter_table_list = []
for alter_table_owner_stmt in tqdm(alter_table_owner_list, desc="Progress"):
    try:
        spark.sql(alter_table_owner_stmt)
    except Exception as e:
        failed_alter_table_list.append(alter_table_owner_stmt)
        print(f"Error altering table owner: {e}")

# COMMAND ----------

if len(failed_alter_table_list) > 0:
    failed_alter_table_df = spark.createDataFrame(failed_alter_table_list, "string")
    display(failed_alter_table_df)

# COMMAND ----------

