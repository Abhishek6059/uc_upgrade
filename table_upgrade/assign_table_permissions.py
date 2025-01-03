# Databricks notebook source
pip install tqdm

# COMMAND ----------

import requests
from tqdm import tqdm
from pyspark.sql.functions import *

# COMMAND ----------

SRC_DATABRICKS_HOST = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
DEST_DATABRICKS_HOST = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
print(SRC_DATABRICKS_HOST)
print(DEST_DATABRICKS_HOST)

# COMMAND ----------

SRC_ACCESS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
SRC_HEADERS = {"Authorization": f"Bearer {SRC_ACCESS_TOKEN}"}
DEST_ACCESS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
DEST_HEADERS = {"Authorization": f"Bearer {DEST_ACCESS_TOKEN}"}
print(SRC_HEADERS)
print(DEST_HEADERS)

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")
print(CATALOG_NAME)

# COMMAND ----------

schema_list = ["corp_de_alyt_hr",
"corp_de_alyt_immuta_output_hr",
"corp_de_hub_hr",
"corp_de_hub_hrpa",
"corp_de_lake_hr",
"corp_de_lake_hrpa",
"corp_de_mart_hrpa",
"corp_de_raw_hr",
"corp_de_raw_hrpa",
"corp_hrpa"]
# schema_list = ["corp_de_alyt_immuta_output_hr"]

# COMMAND ----------

def get_table_list(schema_name):
    schema_tables_dict = {}
    try:
        # spark.sql("USE CATALOG hive_metastore")
        tables_list = [table["tableName"] for table in spark.sql(f"SHOW TABLES IN {schema_name}").collect() if table["isTemporary"] == False]
        schema_tables_dict = {"schema_name": schema_name, "tables_list": tables_list}
    except Exception as e:
        print(f"Error: {e}")
        schema_tables_dict = {"schema_name": schema_name, "tables_list" : []}
    return schema_tables_dict

# COMMAND ----------

schema_tables_list = []
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor() as executor:
    schema_tables_list = list(tqdm(executor.map(get_table_list, schema_list), total=len(schema_list)))
schema_tables_list[:1]

# COMMAND ----------

schema_tables_df = spark.createDataFrame(schema_tables_list)
schema_tables_df = schema_tables_df.withColumn("table_count", size(col("tables_list")))
display(schema_tables_df)

# COMMAND ----------

def get_table_permission_list(schema_name, table_name):
    table_permission_list = []
    try:
        # spark.sql("USE CATALOG hive_metastore")
        table_permission_df = spark.sql(f"SHOW GRANTS ON TABLE hive_metastore.{schema_name}.{table_name}")
        table_permission_df = (
            table_permission_df.groupBy("Principal", "ObjectType").agg(collect_set(col("ActionType")).alias("PermissionList"))
        )
        for table_permission in table_permission_df.collect():
            principal = table_permission["Principal"]
            permission_list = table_permission["PermissionList"]
            object_level = table_permission["ObjectType"]
            table_permission_list.append({"schema_name": schema_name, "table_name": table_name, "principal": principal, "permission_list": permission_list, "object_level": object_level})

    except Exception as e:
        print(f"Error: {e}")
        table_permission_list = [{"schema_name": schema_name, "table_name": table_name}]
    return table_permission_list

# COMMAND ----------

inp_schema_tables_list = [[schema_table["schema_name"], table_name] for schema_table in schema_tables_list for table_name in schema_table["tables_list"]] 
inp_schema_tables_list[:5]

# COMMAND ----------

table_permission_list = []
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor() as executor:
    table_permission_list = list(tqdm(executor.map(lambda x: get_table_permission_list(*x), inp_schema_tables_list), total=len(inp_schema_tables_list)))
table_permission_list[:1]

# COMMAND ----------

if len(table_permission_list) > 0:
    table_permission_df = spark.createDataFrame(table_permission_list[0])
    display(table_permission_df)

# COMMAND ----------

for table_permission in tqdm(table_permission_list[1:], desc="Progress"):
    table_permission_df = table_permission_df.unionByName(spark.createDataFrame(table_permission))
display(table_permission_df)

# COMMAND ----------

def apply_permissions(catalog_name, schema_name, table_name, principal, permission_url, permission_list):
    api_data = {"changes": [{"principal": principal, "add": permission_list}]}
    response = requests.patch(permission_url, headers=DEST_HEADERS, json=api_data)
    json_response = {"catalog_name": catalog_name, "schema_name": schema_name , "table_name": table_name,"principal": principal, "permission_url": permission_url, "permission_list": permission_list}
    if response.status_code == 200:
        json_response["apply_permission_response"] = str(response.json())
    else:
        try:
            json_response["apply_permission_response"] = str(response.json())
        except Exception as e:
            json_response["apply_permission_response"] = "an error occured while applying permissions"
    return json_response

# COMMAND ----------

updated_table_permission_list = []
for table_permission in tqdm(table_permission_df.collect(), desc="Progress"):
    # print(table_permission)
    schema_name = table_permission["schema_name"]
    table_name = table_permission["table_name"]
    principal = table_permission["principal"]
    permission_list = table_permission["permission_list"]
    object_level = table_permission["object_level"]
    # print(object_level)
    tmp_permission_list = []
    # if principal != "users" and table_name == "dev_pvc_immuta_flexgmh":
    if principal != "users":
        if object_level == "CATALOG$":
            permission_url = f"{DEST_DATABRICKS_HOST}/api/2.1/unity-catalog/permissions/catalog/{CATALOG_NAME}"
            for permission in permission_list:
                if permission in ["OWN"]:
                    pass
                if permission == "READ_METADATA":
                    tmp_permission_list.append("BROWSE")
                elif permission == "CREATE_NAMED_FUNCTION":
                    tmp_permission_list.append("CREATE FUNCTION")
                elif permission == "CREATE":
                    tmp_permission_list.extend(["CREATE SCHEMA", "CREATE TABLE"])
                else:
                    tmp_permission_list.append(permission)

        elif object_level == "DATABASE":
            permission_url = f"{DEST_DATABRICKS_HOST}/api/2.1/unity-catalog/permissions/schema/{CATALOG_NAME}.{schema_name}"
            tmp_permission_list = []
            for permission in permission_list:
                if permission in ["READ_METADATA", "OWN"]:
                    pass
                elif permission == "CREATE_NAMED_FUNCTION":
                    tmp_permission_list.append("CREATE FUNCTION")
                elif permission == "CREATE":
                    tmp_permission_list.append("CREATE TABLE")
                else:
                    tmp_permission_list.append(permission)           

        elif object_level == "TABLE":
            permission_url = f"{DEST_DATABRICKS_HOST}/api/2.1/unity-catalog/permissions/table/{CATALOG_NAME}.{schema_name}.{table_name}"
            tmp_permission_list = []
            for permission in permission_list:
                if permission in ["SELECT", "MODIFY"]:
                    tmp_permission_list.append(permission)


        if len(tmp_permission_list) > 0:
            updated_table_permission_list.append([CATALOG_NAME, schema_name, table_name, principal, permission_url, list(set(tmp_permission_list))])

            if object_level == "TABLE":
                schema_permission_url = f"{DEST_DATABRICKS_HOST}/api/2.1/unity-catalog/permissions/schema/{CATALOG_NAME}.{schema_name}"
                updated_table_permission_list.append([CATALOG_NAME, schema_name, table_name, principal, schema_permission_url, ["USAGE"]])

                catalog_permission_url = f"{DEST_DATABRICKS_HOST}/api/2.1/unity-catalog/permissions/catalog/{CATALOG_NAME}"
                updated_table_permission_list.append([CATALOG_NAME, schema_name, table_name, principal, catalog_permission_url, ["USAGE"]])

            elif object_level == "DATABASE":
                catalog_permission_url = f"{DEST_DATABRICKS_HOST}/api/2.1/unity-catalog/permissions/catalog/{CATALOG_NAME}"
                updated_table_permission_list.append([CATALOG_NAME, schema_name, table_name, principal, catalog_permission_url, ["USAGE"]])

updated_table_permission_list[:1]

# COMMAND ----------

if len(updated_table_permission_list) > 0:
    updated_table_permission_schema = "catalog_name STRING, schema_name STRING, table_name STRING, principal STRING, permission_url STRING, permission_list ARRAY<STRING>"
    updated_table_permission_df = spark.createDataFrame(updated_table_permission_list, updated_table_permission_schema)
    updated_table_permission_df = updated_table_permission_df.dropDuplicates(["principal", "permission_url", "permission_list"])
    display(updated_table_permission_df)

# COMMAND ----------

deduped_table_permission_list = [[tbl_perm["catalog_name"], tbl_perm["schema_name"], tbl_perm["table_name"], tbl_perm["principal"], tbl_perm["permission_url"], tbl_perm["permission_list"]] for tbl_perm in updated_table_permission_df.collect()]
deduped_table_permission_list[:1]

# COMMAND ----------

table_permission_response_list = []
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor() as executor:
    table_permission_response_list = list(tqdm(executor.map(lambda x: apply_permissions(*x), deduped_table_permission_list), total=len(deduped_table_permission_list)))
table_permission_response_list[:1]

# COMMAND ----------

if len(table_permission_response_list) > 0:
    table_permission_response_df = spark.createDataFrame(table_permission_response_list)
    display(table_permission_response_df)

# COMMAND ----------

