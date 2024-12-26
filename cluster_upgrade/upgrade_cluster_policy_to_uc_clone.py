# Databricks notebook source
pip install tqdm

# COMMAND ----------

import requests
import json
from tqdm import tqdm
from pyspark.sql.functions import *

# COMMAND ----------

# dbutils.widgets.text("default_catalog", "", "Default Catalog")
# DEFAULT_CATALOG = dbutils.widgets.get("default_catalog")
# dbutils.widgets.text("schema_name", "", "Schema Name")
# SCHEMA_NAME = dbutils.widgets.get("schema_name")
# dbutils.widgets.text("volume_path", "", "Volume Path")
# VOLUME_PATH = dbutils.widgets.get("volume_path")
dbutils.widgets.text("policy_id", "", "Policy ID")
POLICY_ID = dbutils.widgets.get("policy_id")
# print(VOLUME_PATH)
print(POLICY_ID)

# COMMAND ----------

# DATABRICKS_HOST = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
# DATABRICKS_HOST = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
SRC_DATABRICKS_HOST = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
DEST_DATABRICKS_HOST = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
print(SRC_DATABRICKS_HOST)
print(DEST_DATABRICKS_HOST)

# COMMAND ----------

# TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
# HEADERS = {
#     "Authorization": f"Bearer {TOKEN}",
#     "Content-Type": "application/json"
# }
# print(HEADERS)
SRC_ACCESS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
SRC_HEADERS = {"Authorization": f"Bearer {SRC_ACCESS_TOKEN}"}
DEST_ACCESS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
DEST_HEADERS = {"Authorization": f"Bearer {DEST_ACCESS_TOKEN}"}
print(SRC_HEADERS)
print(DEST_HEADERS)

# COMMAND ----------

input_policy_list = POLICY_ID.split(",")
input_policy_list

# COMMAND ----------

import copy
def schema_check(df_lst):
    try:
        tmp_df_lst = copy.deepcopy(df_lst)
    except Exception as e:
        tmp_df_lst = list(df_lst)
    # tmp_df_lst = list(df_lst)
    for element in tmp_df_lst:
        for key, val in element.items():
            element[key] = str(val)
    return tmp_df_lst

# COMMAND ----------

def get_policy_details(api_url, api_headers, api_data):
    response = requests.get(url=api_url, headers=api_headers, json=api_data)
    json_response = {}
    if response.status_code == 200:
        json_response = response.json()
    else:
        print("Request failed with ", response.content)
        try:
            json_response = response.json()
        except Exception as e:
            json_response = {"message": f"an error ocurred while getting cluster policy details for: {api_data}"}
    return json_response

# COMMAND ----------

policy_details_list = []
policy_url = f"{SRC_DATABRICKS_HOST}/api/2.0/policies/clusters/get"
try:
    for policy_id in input_policy_list:
        policy_data = {"policy_id": policy_id}
        policy_resp_dict = get_policy_details(policy_url, SRC_HEADERS, policy_data)
        if "policy_id" in policy_resp_dict:
            policy_details_list.append(policy_resp_dict)
        else:
            print(policy_resp_dict)
    print("Cluster Policies:")
    try:
        policy_df = spark.createDataFrame(policy_details_list)
    except Exception as e:
        policy_df = spark.createDataFrame(schema_check(policy_details_list))
    display(policy_df)
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC - Remove non-compatible policy definition according to UC

# COMMAND ----------

for policy_details_dict in policy_details_list:
    # for policy_key, policy_val in policy_details_dict.items():
    #     print(policy_key, policy_val)
    #     print(type(policy_key), type(policy_val))
    #     print("="*100)
    policy_details_dict["name"] = f'{policy_details_dict["name"]}-uc'

    if "definition" in policy_details_dict:
        policy_definition = policy_details_dict["definition"]
        if isinstance(policy_definition, str):
            policy_definition = json.loads(policy_details_dict["definition"])
        if "spark_conf.spark.databricks.repl.allowedLanguages" in policy_definition:
            policy_definition.pop("spark_conf.spark.databricks.repl.allowedLanguages")

        if "spark.databricks.pyspark.enablePy4JSecurity" in policy_definition:
            policy_definition.pop("spark.databricks.pyspark.enablePy4JSecurity")

        if "spark.databricks.acl.dfAclsEnabled" in policy_definition:
            policy_definition.pop("spark.databricks.acl.dfAclsEnabled")
    
        if "init_scripts.0.workspace.destination" in policy_definition:
            policy_definition.pop("init_scripts.0.workspace.destination")
    
        policy_details_dict["definition"] = json.dumps(policy_definition)
    
print("Updated Cluster Policies:")
try:
    updated_policy_df = spark.createDataFrame(policy_details_list)
except Exception as e:
    updated_policy_df = spark.createDataFrame(schema_check(policy_details_list))
display(updated_policy_df)


# COMMAND ----------

def create_policy(api_url, api_headers, api_data, policy_name):
    response = requests.post(url=api_url, headers=api_headers, json=api_data)
    json_response = {}
    if response.status_code == 200:
        json_response = response.json()
    else:
        print("Request failed with status code", response.text)
        try:
            json_response = response.json()
        except Exception as e:
            json_response = {"message": f"an error ocurred while creating policy with name: {policy_name}"}
    return json_response

# COMMAND ----------

create_policy_resp_list = []
policy_mapping = {}
create_policy_url = f"{DEST_DATABRICKS_HOST}/api/2.0/policies/clusters/create"
for policy_dict in policy_details_list:
    old_policy_id = policy_dict["policy_id"]
    policy_name = policy_dict["name"]
    # print(policy_dict)
    create_policy_resp = create_policy(create_policy_url, DEST_HEADERS, policy_dict, policy_name)
    if "policy_id" in create_policy_resp:
        policy_mapping[policy_name] = {"old_policy_id": old_policy_id, "new_policy_id": create_policy_resp["policy_id"]}
    create_policy_resp_list.append({"policy_name": policy_name, "response": str(create_policy_resp)})

print("Create Cluster Policy Response:")
try:
    create_policy_resp_df = spark.createDataFrame(create_policy_resp_list)
except Exception as e:
    create_policy_resp_df = spark.createDataFrame(schema_check(create_policy_resp_list))
display(create_policy_resp_df)

# COMMAND ----------

create_policy_resp_list

# COMMAND ----------

policy_mapping

# COMMAND ----------

