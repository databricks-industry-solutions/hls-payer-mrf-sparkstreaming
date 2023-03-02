# Databricks notebook source
# MAGIC %md This notebook sets up the companion cluster(s) to run the solution accelerator on a Databricks Workspace. It also creates the Workflow to illustrate the order of execution. Happy exploring! 
# MAGIC 🎉
# MAGIC 
# MAGIC **Steps**
# MAGIC 1. Simply attach this notebook to a cluster and hit Run-All for this notebook. A multi-step job and the clusters used in the job will be created for you and hyperlinks are printed on the last block of the notebook. 
# MAGIC 
# MAGIC 2. Run the accelerator notebooks: Feel free to explore the multi-step job page and **run the Workflow**, or **run the notebooks interactively** with the cluster to see how this solution accelerator executes. 
# MAGIC 
# MAGIC     2a. **Run the Workflow**: Navigate to the Workflow link and hit the `Run Now` 💥. 
# MAGIC   
# MAGIC     2b. **Run the notebooks interactively**: Attach the notebook with the cluster(s) created and execute as described in the `job_json['tasks']` below.
# MAGIC 
# MAGIC **Prerequisites** 
# MAGIC 1. You need to have cluster creation permissions in this workspace.
# MAGIC 
# MAGIC 2. In case the environment has cluster-policies that interfere with automated deployment, you may need to manually create the cluster in accordance with the workspace cluster policy. The `job_json` definition below still provides valuable information about the configuration these series of notebooks should run with. 
# MAGIC 
# MAGIC **Notes**
# MAGIC 1. The pipelines, workflows and clusters created in this script are not user-specific. Keep in mind that rerunning this script again after modification resets them for other users too.
# MAGIC 
# MAGIC 2. If the job execution fails, please confirm that you have set up other environment dependencies as specified in the accelerator notebooks. Accelerators may require the user to set up additional cloud infra or secrets to manage credentials. 

# COMMAND ----------

# DBTITLE 0,Install util packages
# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy@v1.0.13 git+https://github.com/databricks-industry-solutions/notebook-solution-companion@safe-print-html --quiet --disable-pip-version-check

# COMMAND ----------

# MAGIC %sh -e
# MAGIC #Download to DBFS storage
# MAGIC mkdir -p /dbfs/databricks-industry-solutions/hls-payer-mrf-sparkstreaming/
# MAGIC wget https://github.com/databricks-industry-solutions/hls-payer-mrf-sparkstreaming/releases/download/0.3.5v/payer-mrf-streamsource-0.3.5.jar -O /dbfs/databricks-industry-solutions/hls-payer-mrf-sparkstreaming/payer-mrf-streamsource-0.3.5.jar

# COMMAND ----------

from solacc.companion import NotebookSolutionCompanion

# COMMAND ----------

job_json = {
        "timeout_seconds": 28800,
        "max_concurrent_runs": 1,
        "tags": {
            "usage": "solacc_testing",
            "group": "HLS"
        },
        "tasks": [
            {
                "job_cluster_key": "payer_mrf_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"00_README"
                },
                "task_key": "payer_mrf_00",
                "description": ""
            },
            {
                "job_cluster_key": "payer_mrf_cluster",
                "notebook_task": {
                    "notebook_path": f"01_payer_mrf_demo"
                },
                "libraries": [
                    {
                        "jar": "dbfs:/databricks-industry-solutions/hls-payer-mrf-sparkstreaming/payer-mrf-streamsource-0.3.5.jar"
                    }
                ],
                "depends_on": [
                    {
                        "task_key": "payer_mrf_00"
                    }
                ],
                "task_key": "payer_mrf_01",
                "description": ""
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "payer_mrf_cluster",
                "new_cluster": {
                    "spark_version": "10.4.x-scala2.12",
                "spark_conf": {
                    "spark.rpc.message.maxSize": "1024",
                    "spark.driver.cores": "3", # 1 reader, 1 offset writer, 1 for spark tasks
                    "spark.executor.cores": "2",
                    "spark.executor.instances": "4"
                    },
                    "num_workers": 4,
                    "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_L4s", "GCP": "n1-highmem-4"},
                    "custom_tags": {
                        "usage": "solacc_testing"
                    },
                }
            }
        ]
    }

# COMMAND ----------

dbutils.widgets.dropdown("run_job", "False", ["True", "False"])
run_job = dbutils.widgets.get("run_job") == "True"
NotebookSolutionCompanion().deploy_compute(job_json, run_job=run_job)

# COMMAND ----------


