# Databricks notebook source
import requests
import json
from pyspark.sql.functions import *

# COMMAND ----------

# dbutils.widgets.text("volume_path", "", "Volume Path")
# VOLUME_PATH = dbutils.widgets.get("volume_path")
dbutils.widgets.text("job_id", "", "Job Id")
JOB_ID = dbutils.widgets.get("job_id")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")
dbutils.widgets.dropdown("pause_status", "PAUSED", ["PAUSED", "UNPAUSED"], "Pause Status")
PAUSE_STATUS = dbutils.widgets.get("pause_status")
dbutils.widgets.text("run_as_username", "", "Run As User Name")
RUN_AS_USERNAME = dbutils.widgets.get("run_as_username")
dbutils.widgets.text("existing_cluster_id", "", "Existing Cluster Id")
EXISTING_CLUSTER_ID = dbutils.widgets.get("existing_cluster_id")
# print(VOLUME_PATH)
print(JOB_ID)
print(CATALOG_NAME)
print(PAUSE_STATUS)
print(RUN_AS_USERNAME)
print(EXISTING_CLUSTER_ID)

# COMMAND ----------

input_job_list = JOB_ID.split(",")
input_job_list

# COMMAND ----------

# DATABRICKS_HOST = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
SRC_DATABRICKS_HOST = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
DEST_DATABRICKS_HOST = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
print(SRC_DATABRICKS_HOST)
print(DEST_DATABRICKS_HOST)

# COMMAND ----------

# dbutils.widgets.text("ACCESS_TOKEN", "", "Access Token")
# ACCESS_TOKEN = dbutils.widgets.get("ACCESS_TOKEN")
SRC_ACCESS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
SRC_HEADERS = {"Authorization": f"Bearer {SRC_ACCESS_TOKEN}"}
DEST_ACCESS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
DEST_HEADERS = {"Authorization": f"Bearer {DEST_ACCESS_TOKEN}"}
print(SRC_HEADERS)
print(DEST_HEADERS)

# COMMAND ----------

# input_job_list = ["451643523721466"]

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

def get_job_details(api_url, api_headers, api_data):
    response = requests.get(url=api_url, headers=api_headers, json=api_data)
    json_response = {}
    if response.status_code == 200:
        json_response = response.json()
    else:
        print("Request failed with status code", response.content)
        try:
            json_response = response.json()
        except Exception as e:
            json_response = {"message": f"an error ocurred while getting job details for: {api_data}"}
    return json_response

# COMMAND ----------

job_details_list = []
job_url = f"{SRC_DATABRICKS_HOST}/api/2.1/jobs/get"
try:
    for job_id in input_job_list:
        job_data = {"job_id": job_id}
        job_resp_dict = get_job_details(job_url, SRC_HEADERS, job_data)
        if "job_id" in job_resp_dict:
            job_details_settings = job_resp_dict["settings"]
            for job_settings_key, job_settings_val in job_details_settings.items():
                job_resp_dict[job_settings_key] = job_settings_val
            job_resp_dict.pop('settings', None)
            job_details_list.append(job_resp_dict)
        else:
            print(f"{job_data} : {job_resp_dict}")
    print("Jobs:")
    try:
        jobs_df = spark.createDataFrame(jobs_details_list)
    except Exception as e:
        jobs_df = spark.createDataFrame(schema_check(job_details_list))
    display(jobs_df)
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC - Make UC changes to Job config

# COMMAND ----------

for job_details_dict in job_details_list:
    # for job_key, job_val in job_details_dict.items():
    #     print(job_key, job_val)
    #     print(type(job_key), type(job_val))
    #     print("="*100)
    job_details_dict["name"] = f'{job_details_dict["name"]}-UC'

    if PAUSE_STATUS == "PAUSED":
        if "schedule" in job_details_dict:
            if "pause_status" in job_details_dict["schedule"]:
                job_details_dict["schedule"]["pause_status"] = "PAUSED"

        if "continuous" in job_details_dict:
            if "pause_status" in job_details_dict["continuous"]:
                job_details_dict["continuous"]["pause_status"] = "PAUSED"
    else:
        if "schedule" in job_details_dict:
            if "pause_status" in job_details_dict["schedule"]:
                job_details_dict["schedule"]["pause_status"] = "UNPAUSED"

        if "continuous" in job_details_dict:
            if "pause_status" in job_details_dict["continuous"]:
                job_details_dict["continuous"]["pause_status"] = "UNPAUSED"

    if "run_as_user_name" in job_details_dict:
        if RUN_AS_USERNAME.strip() == "":
            if "@" in job_details_dict["run_as_user_name"]:
                job_details_dict["run_as"] = {"user_name": job_details_dict["run_as_user_name"]}
            else:
                job_details_dict["run_as"] = {"service_principal_name": job_details_dict["run_as_user_name"]}
        else:
            if "@" in RUN_AS_USERNAME:
                job_details_dict["run_as"] = {"user_name": RUN_AS_USERNAME}
            else:
                job_details_dict["run_as"] = {"service_principal_name": RUN_AS_USERNAME}

    job_cluster_list = job_details_dict.get("job_clusters", [])
    for ind, job_cluster in enumerate(job_cluster_list):
        if "new_cluster" in job_cluster:
            if "spark_version" in job_cluster["new_cluster"]:
                # job_cluster["new_cluster"]["spark_version"] = "13.3.x-scala2.12"

                if "custom" not in job_cluster["new_cluster"]["spark_version"]:
                    spark_version_nbr = job_cluster["new_cluster"]["spark_version"].split(".x")[0]
                    if float(spark_version_nbr) >= 13.3:
                        job_cluster["new_cluster"]["spark_version"] = f"{spark_version_nbr}.x{job_cluster['new_cluster']['spark_version'].split('.x')[-1]}"
                    else:
                        job_cluster["new_cluster"]["spark_version"] = f"13.3.x{job_cluster['new_cluster']['spark_version'].split('.x')[-1]}"
                else:
                    if "ml" in job_cluster["new_cluster"]["spark_version"]:
                        if "gpu" in job_cluster["new_cluster"]["spark_version"]:
                            job_cluster["new_cluster"]["spark_version"] = "13.3.x-gpu-ml-scala2.12"
                        else:
                            job_cluster["new_cluster"]["spark_version"] = "13.3.x-cpu-ml-scala2.12"
                    else:
                        job_cluster["new_cluster"]["spark_version"] = "13.3.x-scala2.12"

            # if "data_security_mode" in job_cluster["new_cluster"]:
            #     if "SINGLE_USER" in job_cluster["new_cluster"]["data_security_mode"]:
            #         job_cluster["new_cluster"]["data_security_mode"] = "SINGLE_USER"
            #     else:
            #         job_cluster["new_cluster"]["data_security_mode"] = "USER_ISOLATION"
            job_cluster["new_cluster"]["data_security_mode"] = "SINGLE_USER"

            if "spark_conf" in job_cluster["new_cluster"]:
                if "spark.databricks.repl.allowedLanguages" in job_cluster["new_cluster"]["spark_conf"]:
                    job_cluster["new_cluster"]["spark_conf"].pop("spark.databricks.repl.allowedLanguages")
        
                if "spark.databricks.acl.dfAclsEnabled" in job_cluster["new_cluster"]["spark_conf"]:
                    job_cluster["new_cluster"]["spark_conf"].pop("spark.databricks.acl.dfAclsEnabled")

                if "spark.databricks.pyspark.enablePy4JSecurity" in job_cluster["new_cluster"]["spark_conf"]:
                    job_cluster["new_cluster"]["spark_conf"].pop("spark.databricks.pyspark.enablePy4JSecurity")

                if CATALOG_NAME.strip() != "":
                    job_cluster["new_cluster"]["spark_conf"]["spark.databricks.sql.initial.catalog.name"] = CATALOG_NAME

            else:
                if CATALOG_NAME.strip() != "":
                    job_cluster["spark_conf"] = {"spark.databricks.sql.initial.catalog.name" : CATALOG_NAME}
            
            if "aws_attributes" in job_cluster["new_cluster"] and ("ebs_volume_count" in job_cluster["new_cluster"]["aws_attributes"] or "ebs_volume_type" in job_cluster["new_cluster"]["aws_attributes"] or "ebs_volume_size" in job_cluster["new_cluster"]["aws_attributes"]):
                if "disk_spec" in job_cluster["new_cluster"]:
                    job_cluster["new_cluster"].pop("disk_spec")
            
            # if "policy_id" in job_cluster["new_cluster"]:
            #     job_cluster["new_cluster"].pop("policy_id")
            #     # job_cluster["new_cluster"]["policy_id"] = "0009D21EE78E7E83"

    task_list = job_details_dict.get("tasks", [])
    # if isinstance(task_list, str):
    #     task_list = json.loads(json.dumps(job_details_dict["tasks"]))
    # print(type(task_list))
    for ind, task in enumerate(task_list):
        # print(type(task))

        if "existing_cluster_id" in task:
            if EXISTING_CLUSTER_ID.strip() != "":
                task["existing_cluster_id"] = EXISTING_CLUSTER_ID

        if "new_cluster" in task:
            if "spark_version" in task["new_cluster"]:
                task["new_cluster"]["spark_version"] = "13.3.x-scala2.12"

                if "custom" not in task["new_cluster"]["spark_version"]:
                    spark_version_nbr = task["new_cluster"]["spark_version"].split(".x")[0]
                    if float(spark_version_nbr) >= 13.3:
                        task["new_cluster"]["spark_version"] = f"{spark_version_nbr}.x{task['new_cluster']['spark_version'].split('.x')[-1]}"
                    else:
                        task["new_cluster"]["spark_version"] = f"13.3.x{task['new_cluster']['spark_version'].split('.x')[-1]}"
                else:
                    if "ml" in task["new_cluster"]["spark_version"]:
                        if "gpu" in task["new_cluster"]["spark_version"]:
                            task["new_cluster"]["spark_version"] = "13.3.x-gpu-ml-scala2.12"
                        else:
                            task["new_cluster"]["spark_version"] = "13.3.x-cpu-ml-scala2.12"
                    else:
                        task["new_cluster"]["spark_version"] = "13.3.x-scala2.12"

            # if "data_security_mode" in task["new_cluster"]:
            #     if "SINGLE_USER" in task["new_cluster"]["data_security_mode"]:
            #         task["new_cluster"]["data_security_mode"] = "SINGLE_USER"
            #     else:
            #         task["new_cluster"]["data_security_mode"] = "USER_ISOLATION"
            task["new_cluster"]["data_security_mode"] = "SINGLE_USER"

            if "spark_conf" in task["new_cluster"]:
                if "spark.databricks.repl.allowedLanguages" in task["new_cluster"]["spark_conf"]:
                    task["new_cluster"]["spark_conf"].pop("spark.databricks.repl.allowedLanguages")
        
                if "spark.databricks.acl.dfAclsEnabled" in task["new_cluster"]["spark_conf"]:
                    task["new_cluster"]["spark_conf"].pop("spark.databricks.acl.dfAclsEnabled")

                if "spark.databricks.pyspark.enablePy4JSecurity" in task["new_cluster"]["spark_conf"]:
                    task["new_cluster"]["spark_conf"].pop("spark.databricks.pyspark.enablePy4JSecurity")

                if CATALOG_NAME.strip() != "":
                    task["new_cluster"]["spark_conf"]["spark.databricks.sql.initial.catalog.name"] = CATALOG_NAME

            else:
                if CATALOG_NAME.strip() != "":
                    task["new_cluster"]["spark_conf"] = {"spark.databricks.sql.initial.catalog.name" : CATALOG_NAME}

            if "aws_attributes" in task["new_cluster"] and ("ebs_volume_count" in task["new_cluster"]["aws_attributes"] or "ebs_volume_type" in task["new_cluster"]["aws_attributes"] or "ebs_volume_size" in task["new_cluster"]["aws_attributes"]):
                if "disk_spec" in task["new_cluster"]:
                    task["new_cluster"].pop("disk_spec")

            # if "policy_id" in task["new_cluster"]:
            #     task["new_cluster"].pop("policy_id")
            #     # task["new_cluster"]["policy_id"] = "0009D21EE78E7E83"

print("Updated Jobs:")
try:
    updated_jobs_df = spark.createDataFrame(job_details_list)
except Exception as e:
    updated_jobs_df = spark.createDataFrame(schema_check(job_details_list))
display(updated_jobs_df)

# COMMAND ----------

def create_job(api_url, api_headers, api_data, job_name):
    response = requests.post(url=api_url, headers=api_headers, json=api_data)
    json_response = {}
    if response.status_code == 200:
        json_response = response.json()
    else:
        print("Request failed with status code", response.text)
        try:
            json_response = response.json()
        except Exception as e:
            json_response = {"message": f"an error ocurred while creating job with name: {job_name}"}
    return json_response

# COMMAND ----------

create_job_resp_list = []
job_mapping = {}
create_job_url = f"{DEST_DATABRICKS_HOST}/api/2.1/jobs/create"
for job_dict in job_details_list:
    old_job_id = job_dict["job_id"]
    job_name = job_dict["name"]
    # print(job_dict)
    create_job_resp = create_job(create_job_url, DEST_HEADERS, job_dict, job_name)
    if "job_id" in create_job_resp:
        job_mapping[job_name] = {"old_job_id": old_job_id, "new_job_id": create_job_resp["job_id"]}
        create_job_resp_list.append({"job_name": job_name, "old_job_id": old_job_id, "new_job_id": create_job_resp["job_id"], "response": str(create_job_resp)})
    else:
        create_job_resp_list.append({"job_name": job_name, "old_job_id": old_job_id, "response": str(create_job_resp)})

print("Create Job Response:")
try:
    create_job_resp_df = spark.createDataFrame(create_job_resp_list)
except Exception as e:
    create_job_resp_df = spark.createDataFrame(schema_check(create_job_resp_list))
display(create_job_resp_df)

# COMMAND ----------

def get_job_permissions(api_url, api_headers, job_id):
    # api_data = {"job_id": str(job_id)}
    response = requests.get(url=api_url, headers=api_headers)
    json_response = {}
    if response.status_code == 200:
        json_response = response.json()
    else:
        try:
            json_response = response.json()
        except Exception as e:
            json_response = {"message": f"an error ocurred while getting job permissions: {job_id}"}
    return json_response

# COMMAND ----------

permission_mapping_list = []
new_job_id = None
if len(create_job_resp_list) > 0:
    for job_resp in create_job_resp_list:
        if "old_job_id" in job_resp:
            old_job_id = job_resp["old_job_id"]
        if "new_job_id" in job_resp:
            new_job_id = job_resp["new_job_id"]
        if "job_name" in job_resp:
            job_name = job_resp["job_name"]
        job_permission_url = f"{SRC_DATABRICKS_HOST}/api/2.0/permissions/jobs/{old_job_id}"    
        job_permission_resp = get_job_permissions(job_permission_url, SRC_HEADERS, old_job_id)
        # print(job_permission_resp)
        if "object_id" in job_permission_resp:
            if "access_control_list" in job_permission_resp:
                permissions_list = []
                for permission in job_permission_resp["access_control_list"]:
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
                        
                permission_mapping_list.append({"old_job_id": old_job_id, "new_job_id": new_job_id, "job_name": job_name, "access_control_list": permissions_list})
        
    print("Cluster Permission:")
    if len(permission_mapping_list) > 0:
        try:
            permission_mapping_df = spark.createDataFrame(permission_mapping_list)
        except Exception as e:
            permission_mapping_df = spark.createDataFrame(schema_check(permission_mapping_list))
        display(permission_mapping_df)
# permission_mapping_list[:1]

# COMMAND ----------

def sync_permissions(api_url, api_headers, job_id, access_control_list):
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
            json_response = {"message": f"an error ocurred while syncing permissions: {job_id}"}
    return json_response

# COMMAND ----------

permission_sync_resp_list = []
for permission_details in permission_mapping_list:
    new_job_id = permission_details["new_job_id"]
    old_job_id = permission_details["old_job_id"]
    job_name = permission_details["job_name"]
    access_control_list = permission_details["access_control_list"]
    permission_sync_url = f"{DEST_DATABRICKS_HOST}/api/2.0/permissions/jobs/{new_job_id}"
    permission_sync_resp = sync_permissions(permission_sync_url, DEST_HEADERS, new_job_id, access_control_list)
    permission_sync_resp_list.append({"new_job_id": new_job_id, "job_name": job_name, "old_job_id": old_job_id, "access_control_list": access_control_list, "permission_sync_resp": str(permission_sync_resp)})

print("Permission Sync Responses:")
if len(permission_sync_resp_list) > 0:
    try:
        permission_sync_resp_df = spark.createDataFrame(permission_sync_resp_list)
    except Exception as e:
        permission_sync_resp_df = spark.createDataFrame(schema_check(permission_sync_resp_list))
    display(permission_sync_resp_df)

# COMMAND ----------

