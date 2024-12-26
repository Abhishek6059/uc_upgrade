# Databricks notebook source
# MAGIC %md
# MAGIC ##### For Shared Mode -
# MAGIC - Download the init_script from workspace files and upload it to the volume
# MAGIC - Note : create a volume as utility_scripts under default schema of respective BU catalog , if not already exists
# MAGIC - Add the init script path to the metastore allowed list

# COMMAND ----------

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
dbutils.widgets.text("volume_path", "", "Volume Path")
VOLUME_PATH = dbutils.widgets.get("volume_path")
dbutils.widgets.text("single_user", "", "Single User")
SINGLE_USER = dbutils.widgets.get("single_user")
dbutils.widgets.text("policy_id", "", "Policy ID")
POLICY_ID = dbutils.widgets.get("policy_id")
dbutils.widgets.text("cluster_id", "", "Cluster ID")
CLUSTER_ID = dbutils.widgets.get("cluster_id")
print(VOLUME_PATH)
print(SINGLE_USER)
print(POLICY_ID)
print(CLUSTER_ID)

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
# print(DATABRICKS_HOST)
# print(HEADERS)
SRC_ACCESS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
SRC_HEADERS = {"Authorization": f"Bearer {SRC_ACCESS_TOKEN}"}
DEST_ACCESS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
DEST_HEADERS = {"Authorization": f"Bearer {DEST_ACCESS_TOKEN}"}
print(SRC_HEADERS)
print(DEST_HEADERS)

# COMMAND ----------

# input_cluster_list = ["1001-215340-xl52r6lu"]
input_cluster_list = CLUSTER_ID.split(",")
print(input_cluster_list)

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

def get_cluster_details(api_url, api_headers, api_data):
    response = requests.get(url=api_url, headers=api_headers, json=api_data)
    json_response = {}
    if response.status_code == 200:
        json_response = response.json()
    else:
        print("Request failed with status code", response.content)
        try:
            json_response = response.json()
        except Exception as e:
            json_response = {"message": f"an error ocurred while getting cluster details for: {api_data}"}
    return json_response

# COMMAND ----------

cluster_details_list = []
cluster_url = f"{SRC_DATABRICKS_HOST}/api/2.0/clusters/get"
try:
    for cluster_id in input_cluster_list:
        cluster_data = {"cluster_id": cluster_id}
        cluster_resp_dict = get_cluster_details(cluster_url, SRC_HEADERS, cluster_data)
        if "cluster_id" in cluster_resp_dict:
            cluster_details_list.append(cluster_resp_dict)
        else:
            print(cluster_resp_dict)
    print("Clusters:")
    try:
        clusters_df = spark.createDataFrame(cluster_details_list)
    except Exception as e:
        clusters_df = spark.createDataFrame(schema_check(cluster_details_list))
    display(clusters_df)
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC - Make UC changes to cluster config

# COMMAND ----------

for cluster_details_dict in cluster_details_list:
    # for cluster_key, cluster_val in cluster_details_dict.items():
        # print(cluster_key, cluster_val)
        # print(type(cluster_key), type(cluster_val))
        # print("="*100)
    # cluster_details_dict["cluster_name"] = f'{cluster_details_dict["cluster_name"].rstrip("-UC")}-UC'
    cluster_details_dict["cluster_name"] = f'{cluster_details_dict["cluster_name"]}-UC'

    # cluster_details_dict["spark_version"] = "13.3.x-scala2.12"
    if "custom" not in cluster_details_dict["spark_version"]:
        spark_version_nbr = cluster_details_dict["spark_version"].split(".x")[0]
        # print(spark_version_nbr)
        if float(spark_version_nbr) >= 13.3:
            cluster_details_dict["spark_version"] = f"{spark_version_nbr}.x{cluster_details_dict['spark_version'].split('.x')[-1]}"
        else:
            cluster_details_dict["spark_version"] = f"13.3.x{cluster_details_dict['spark_version'].split('.x')[-1]}"
    else:
        if "ml" in cluster_details_dict["spark_version"]:
            cluster_details_dict["spark_version"] = "13.3.x-cpu-ml-scala2.12"
        else:
            cluster_details_dict["spark_version"] = "13.3.x-scala2.12"

    if "ml" in cluster_details_dict["spark_version"]:
        if SINGLE_USER.strip("") != "":
            cluster_details_dict["single_user_name"] = SINGLE_USER
        
    if "ml" not in cluster_details_dict["spark_version"]:
        if "SINGLE_USER" in cluster_details_dict.get("data_security_mode", ""):
            cluster_details_dict["data_security_mode"] = "SINGLE_USER"
        else:
            cluster_details_dict["data_security_mode"] = "USER_ISOLATION"
    else:
        cluster_details_dict["data_security_mode"] = "SINGLE_USER"

    if "single_user_name" not in cluster_details_dict:
        if "init_scripts" in cluster_details_dict:
            init_script_list = cluster_details_dict["init_scripts"]
            # print(init_script_list)
            tmp_init_script_list = []
            for init_script in init_script_list:
                if "workspace" in init_script:
                    init_script_file_name = init_script["workspace"]["destination"].split("/")[-1]
                    tmp_init_script_list.append({"volumes" : {"destination" : f'{VOLUME_PATH.rstrip("/")}/{init_script_file_name}'}})
                elif "volumes" in init_script:
                    tmp_init_script_list.append({"volumes" : {"destination" : init_script["volumes"]["destination"]}})
                elif "s3" in init_script:
                    init_script_file_name = init_script["s3"]["destination"].split("/")[-1]
                    tmp_init_script_list.append({"s3" : {"destination" : init_script["s3"]["destination"]}})

            cluster_details_dict["init_scripts"] = tmp_init_script_list

    # if DEFAULT_CATALOG.strip() != "":
    #     if 'spark_conf' in cluster_details_dict:
    #         cluster_details_dict['spark_conf']["spark.databricks.sql.initial.catalog.name"] = DEFAULT_CATALOG
    #     else:
    #         cluster_details_dict['spark_conf'] = {'spark.databricks.sql.initial.catalog.name': DEFAULT_CATALOG}

    if "spark_conf" in cluster_details_dict:
        if "spark.databricks.repl.allowedLanguages" in cluster_details_dict["spark_conf"]:
            cluster_details_dict["spark_conf"].pop("spark.databricks.repl.allowedLanguages")
        
        if "spark.databricks.acl.dfAclsEnabled" in cluster_details_dict["spark_conf"]:
            cluster_details_dict["spark_conf"].pop("spark.databricks.acl.dfAclsEnabled")

        if "spark.databricks.pyspark.enablePy4JSecurity" in cluster_details_dict["spark_conf"]:
            cluster_details_dict["spark_conf"].pop("spark.databricks.pyspark.enablePy4JSecurity")

    if "aws_attributes" in cluster_details_dict and ("ebs_volume_count" in cluster_details_dict["aws_attributes"] or "ebs_volume_type" in cluster_details_dict["aws_attributes"] or "ebs_volume_size" in cluster_details_dict["aws_attributes"]):
        if "disk_spec" in cluster_details_dict:
            cluster_details_dict.pop("disk_spec")
    
    if "policy_id" in cluster_details_dict:
        if POLICY_ID.strip() != "":
            cluster_details_dict["policy_id"] = POLICY_ID
        # else:
        #     cluster_details_dict.pop("policy_id")

print("Updated Clusters:")
try:
    updated_clusters_df = spark.createDataFrame(cluster_details_list)
except Exception as e:
    updated_clusters_df = spark.createDataFrame(schema_check(cluster_details_list))
display(updated_clusters_df)


# COMMAND ----------

def create_cluster(api_url, api_headers, api_data, cluster_name):
    response = requests.post(url=api_url, headers=api_headers, json=api_data)
    json_response = {}
    if response.status_code == 200:
        json_response = response.json()
    else:
        print("Request failed with status code", response.text)
        try:
            json_response = response.json()
        except Exception as e:
            json_response = {"message": f"an error ocurred while creating cluster with name: {cluster_name}"}
    return json_response

# COMMAND ----------

create_cluster_resp_list = []
cluster_mapping = {}
create_cluster_url = f"{DEST_DATABRICKS_HOST}/api/2.1/clusters/create"
for cluster_dict in cluster_details_list:
    old_cluster_id = cluster_dict["cluster_id"]
    cluster_name = cluster_dict["cluster_name"]
    # print(cluster_dict)
    create_cluster_resp = create_cluster(create_cluster_url, DEST_HEADERS, cluster_dict, cluster_name)
    if "cluster_id" in create_cluster_resp:
        cluster_mapping[cluster_name] = {"old_cluster_id": old_cluster_id, "new_cluster_id": create_cluster_resp["cluster_id"]}
    create_cluster_resp_list.append({"cluster_name": cluster_name, "response": str(create_cluster_resp)})

print("Create Cluster Response:")
try:
    create_clusters_resp_df = spark.createDataFrame(create_cluster_resp_list)
except Exception as e:
    create_clusters_resp_df = spark.createDataFrame(schema_check(create_cluster_resp_list))
display(create_clusters_resp_df)

# COMMAND ----------

# create_cluster_resp_list

# COMMAND ----------

# cluster_mapping

# COMMAND ----------

def terminate_cluster(api_url, api_headers, cluster_id):
    api_data = {"cluster_id": str(cluster_id)}
    response = requests.post(url=api_url, headers=api_headers, json=api_data)
    json_response = {}
    if response.status_code == 200:
        json_response = response.json()
    else:
        print("Request failed with status code", response.content)
        try:
            json_response = response.json()
        except Exception as e:
            json_response = {"message": f"an error ocurred while terminating cluster: {cluster_id}"}
    return json_response

# COMMAND ----------

terminate_cluster_resp_list = []
terminate_cluster_url = f"{DEST_DATABRICKS_HOST}/api/2.1/clusters/delete"
if len(cluster_mapping) > 0:
    for cluster_name, cluster_info in cluster_mapping.items():
        new_cluster_id = cluster_info["new_cluster_id"]
        # print(new_cluster_id)
        terminate_cluster_resp = terminate_cluster(terminate_cluster_url, DEST_HEADERS, new_cluster_id)
        terminate_cluster_resp_list.append({"cluster_name": cluster_name, "response": str(terminate_cluster_resp)})

print("Terminate Cluster Responses:")
try:
    terminate_clusters_resp_df = spark.createDataFrame(terminate_cluster_resp_list)
except Exception as e:
    terminate_clusters_resp_df = spark.createDataFrame(schema_check(terminate_cluster_resp_list))
display(terminate_clusters_resp_df)

# COMMAND ----------

def get_libraries(api_url, api_headers, cluster_id):
    api_data = {"cluster_id": str(cluster_id)}
    response = requests.get(url=api_url, headers=api_headers, json=api_data)
    json_response = {}
    if response.status_code == 200:
        json_response = response.json()
    else:
        try:
            json_response = response.json()
        except Exception as e:
            json_response = {"message": f"an error ocurred while getting libraries: {cluster_id}"}
    return json_response

# COMMAND ----------

libraries_mapping_list = []
for cluster_name, cluster_info in cluster_mapping.items():
    old_cluster_id = cluster_info["old_cluster_id"]
    # print(old_cluster_id)
    new_cluster_id = cluster_info["new_cluster_id"]
    libraries_url = f"{SRC_DATABRICKS_HOST}/api/2.0/libraries/cluster-status"
    libraries_resp = get_libraries(libraries_url, SRC_HEADERS, old_cluster_id)
    if "cluster_id" in libraries_resp:
        if "library_statuses" in libraries_resp:
            libraries_list = []
            library_statuses = libraries_resp["library_statuses"]
            for library_status in library_statuses:
                if "library" in library_status:
                    libraries_list.append(library_status["library"])
            libraries_mapping_list.append({"old_cluster_id": old_cluster_id, "new_cluster_id": new_cluster_id, "cluster_name": cluster_name, "libraries_list": libraries_list})
    
print("Cluster Library Responses:")
if len(libraries_mapping_list) > 0:
    try:
        cluster_library_resp_df = spark.createDataFrame(libraries_mapping_list)
    except Exception as e:
        cluster_library_resp_df = spark.createDataFrame(schema_check(libraries_mapping_list))
    display(cluster_library_resp_df)
libraries_mapping_list[:1]

# COMMAND ----------

# MAGIC %md
# MAGIC - Maven or Jar
# MAGIC - If jar, use dbutils to copy jar from dbfs to volume
# MAGIC - Add new jar path and maven to allowed list

# COMMAND ----------

def sync_libraries(api_url, api_headers, cluster_id, libraries_list):
    api_data = {"cluster_id": str(cluster_id), "libraries": libraries_list}
    # print(api_data)
    response = requests.post(url=api_url, headers=api_headers, json=api_data)
    json_response = {}
    if response.status_code == 200:
        json_response = response.json()
    else:
        try:
            json_response = response.json()
        except Exception as e:
            json_response = {"message": f"an error ocurred while syncing libraries: {cluster_id}"}
    return json_response

# COMMAND ----------

sync_libraries_list = []
for cluster_library_details in libraries_mapping_list:
    new_cluster_id = cluster_library_details["new_cluster_id"]
    old_cluster_id = cluster_library_details["old_cluster_id"]
    libraries_list = cluster_library_details["libraries_list"]
    sync_libraries_url = f"{DEST_DATABRICKS_HOST}/api/2.0/libraries/install"
    sync_libraries_resp = sync_libraries(sync_libraries_url, DEST_HEADERS, new_cluster_id, libraries_list)
    sync_libraries_list.append({"new_cluster_id": new_cluster_id, "old_cluster_id": old_cluster_id, "libraries_list": libraries_list, "sync_libraries_resp": str(sync_libraries_resp)})

print("Library Sync Responses:")
if len(sync_libraries_list) > 0:
    try:
        library_sync_resp_df = spark.createDataFrame(sync_libraries_list)
    except Exception as e:
        library_sync_resp_df = spark.createDataFrame(schema_check(sync_libraries_list))
    display(library_sync_resp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - PIN the cluster
# MAGIC - Start and check the cluster for any issues

# COMMAND ----------

def pin_cluster(api_url, api_headers, cluster_id):
    api_data = {"cluster_id": str(cluster_id)}
    response = requests.post(url=api_url, headers=api_headers, json=api_data)
    json_response = {}
    if response.status_code == 200:
        json_response = response.json()
    else:
        try:
            json_response = response.json()
        except Exception as e:
            json_response = {"message": f"an error ocurred while pinning cluster: {cluster_id}"}
    return json_response

# COMMAND ----------

pin_cluster_resp_list = []
pin_cluster_url = f"{DEST_DATABRICKS_HOST}/api/2.1/clusters/pin"
if len(cluster_mapping) > 0:
    for cluster_name, cluster_info in cluster_mapping.items():
        new_cluster_id = cluster_info["new_cluster_id"]
        # print(new_cluster_id)
        pin_cluster_resp = pin_cluster(pin_cluster_url, DEST_HEADERS, new_cluster_id)
        pin_cluster_resp_list.append({"cluster_name": cluster_name, "response": str(pin_cluster_resp)})

print("Pin Cluster Responses:")
try:
    pin_clusters_resp_df = spark.createDataFrame(pin_cluster_resp_list)
except Exception as e:
    pin_clusters_resp_df = spark.createDataFrame(schema_check(pin_cluster_resp_list))
display(pin_clusters_resp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - Sync Permissions

# COMMAND ----------

def get_cluster_permissions(api_url, api_headers, cluster_id):
    # api_data = {"cluster_id": str(cluster_id)}
    response = requests.get(url=api_url, headers=api_headers)
    json_response = {}
    if response.status_code == 200:
        json_response = response.json()
    else:
        try:
            json_response = response.json()
        except Exception as e:
            json_response = {"message": f"an error ocurred while getting cluster permissions: {cluster_id}"}
    return json_response

# COMMAND ----------

permission_mapping_list = []
for cluster_name, cluster_info in cluster_mapping.items():
    old_cluster_id = cluster_info["old_cluster_id"]
    # print(old_cluster_id)
    new_cluster_id = cluster_info["new_cluster_id"]
    cluster_permission_url = f"{SRC_DATABRICKS_HOST}/api/2.0/permissions/clusters/{old_cluster_id}"    
    cluster_permission_resp = get_cluster_permissions(cluster_permission_url, SRC_HEADERS, old_cluster_id)
    print(cluster_permission_resp)
    if "object_id" in cluster_permission_resp:
        if "access_control_list" in cluster_permission_resp:
            permissions_list = []
            for permission in cluster_permission_resp["access_control_list"]:
                if "all_permissions" in permission:
                    all_permission_list = permission["all_permissions"]
                    for all_permission in all_permission_list:
                        if "permission_level" in all_permission:
                            if "inherited" in all_permission:
                                if all_permission["inherited"] != True:
                                    if "group_name" in permission:
                                        permissions_list.append({"group_name": permission["group_name"], "permission_level": all_permission["permission_level"]})
                                    if "user_name" in permission:
                                        permissions_list.append({"user_name": permission["user_name"], "permission_level": all_permission["permission_level"]})
                                    if "service_principal_name" in permission:
                                        permissions_list.append({"service_principal_name": permission["service_principal_name"], "permission_level": all_permission["permission_level"]})
                    
            permission_mapping_list.append({"old_cluster_id": old_cluster_id, "new_cluster_id": new_cluster_id, "cluster_name": cluster_name, "access_control_list": permissions_list})
    
print("Cluster Permission:")
if len(permission_mapping_list) > 0:
    try:
        permission_mapping_df = spark.createDataFrame(permission_mapping_list)
    except Exception as e:
        permission_mapping_df = spark.createDataFrame(schema_check(permission_mapping_list))
    display(permission_mapping_df)
# permission_mapping_list[:1]

# COMMAND ----------

def sync_permissions(api_url, api_headers, cluster_id, access_control_list):
    api_data = {"access_control_list": access_control_list}
    # print(api_data)
    response = requests.put(url=api_url, headers=api_headers, json=api_data)
    json_response = {}
    if response.status_code == 200:
        json_response = response.json()
    else:
        try:
            json_response = response.json()
        except Exception as e:
            json_response = {"message": f"an error ocurred while syncing permissions: {cluster_id}"}
    return json_response

# COMMAND ----------

permission_sync_resp_list = []
for permission_details in permission_mapping_list:
    new_cluster_id = permission_details["new_cluster_id"]
    old_cluster_id = permission_details["old_cluster_id"]
    cluster_name = permission_details["cluster_name"]
    access_control_list = permission_details["access_control_list"]
    permission_sync_url = f"{DEST_DATABRICKS_HOST}/api/2.0/permissions/clusters/{new_cluster_id}"
    permission_sync_resp = sync_permissions(permission_sync_url, DEST_HEADERS, new_cluster_id, access_control_list)
    permission_sync_resp_list.append({"new_cluster_id": new_cluster_id, "cluster_name": cluster_name, "old_cluster_id": old_cluster_id, "access_control_list": access_control_list, "permission_sync_resp": str(permission_sync_resp)})

print("Permission Sync Responses:")
if len(permission_sync_resp_list) > 0:
    try:
        permission_sync_resp_df = spark.createDataFrame(permission_sync_resp_list)
    except Exception as e:
        permission_sync_resp_df = spark.createDataFrame(schema_check(permission_sync_resp_list))
    display(permission_sync_resp_df)

# COMMAND ----------

