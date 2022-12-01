# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/hls-payer-mrf-sparkstreaming. 

# COMMAND ----------

# MAGIC %md 
# MAGIC # SparkStreamSources
# MAGIC Spark Custom Stream Source and Sink for Payer MRF Use Case
# MAGIC 
# MAGIC ## Recommended Spark Settings
# MAGIC 
# MAGIC ``` python
# MAGIC #Spark Settings
# MAGIC spark.rpc.message.maxSize 1024
# MAGIC spark.driver.memory 12g
# MAGIC spark.driver.cores 2
# MAGIC 
# MAGIC #JVM Settings (8g or higher)
# MAGIC JAVA_OPTS=-Xmx8g -Xms8g
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC ## Running
# MAGIC 
# MAGIC ``` python
# MAGIC df = spark.readStream
# MAGIC     .format("com.databricks.labs.sparkstreaming.jsonmrf.JsonMRFSourceProvider")
# MAGIC     .load("/Users/aaron.zavora//Downloads/umr-tpa-encore-in-network-rates.json")
# MAGIC 
# MAGIC df.writeStream
# MAGIC     .outputMode("append")
# MAGIC     .format("text")
# MAGIC     .queryName("umr-tpa-in-network-parsing")
# MAGIC     .option("checkpointLocation", "src/test/resources/chkpoint_dir")
# MAGIC     .start("src/test/resources/output")
# MAGIC ``` 
# MAGIC 
# MAGIC ## Use Case 
# MAGIC 
# MAGIC Schema definition that is parsed is the CMS in-network file. https://github.com/CMSgov/price-transparency-guide/tree/master/schemas/in-network-rates
# MAGIC 
# MAGIC ## Unzipping First Recommended
# MAGIC 
# MAGIC 
# MAGIC ```python
# MAGIC #3.6G zipped, 120G unzipped file 
# MAGIC #download to local storage "Command took 17.75 minutes"
# MAGIC wget -O ./2022-08-01_umr_inc_tpa_encore_non_evaluated_gap_enc-in-network-rates.json.gz https://uhc-tic-mrf.azureedge.net/public-mrf/2022-08-01/2022-08-01_UMR--Inc-_TPA_ENCORE-ENTERPRISES-AIRROSTI-DCI_TX-DALLAS-NON-EVALUATED-GAP_-ENC_NXBJ_in-network-rates.json.gz
# MAGIC 
# MAGIC #unzip to DBFS  "Command took 26.83 minutes"
# MAGIC gunzip -cd ./2022-08-01_umr_inc_tpa_encore_non_evaluated_gap_enc-in-network-rates.json.gz > /dbfs/user/hive/warehouse/hls_dev_payer_transparency.db/raw_files/2022-08-01_umr_inc_tpa_encore_non_evaluated_gap_enc-in-network-rates.json 
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC ## Data Output
# MAGIC 
# MAGIC ``` bash
# MAGIC more  src/test/resources/output/part-00000-a6af8cf3-6162-4d60-9acb-8933bac19b8b-c000.txt
# MAGIC >[{"negotiation_arrangement":"ffs","name":"BRONCHOSCOPY W/TRANSBRONCHIAL LUNG BX EACH LOBE","billi
# MAGIC ...
# MAGIC >[{"negotiation_arrangement":"ffs","name":"ANESTHESIA EXTENSIVE SPINE & SPINAL CORD","bil
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC ## Speed 
# MAGIC 
# MAGIC On a Spark driver with xmx8g performs at ~2.5GB per minute. Note of caution, this program depends on buffering. Some forms of .gz extension do not enable efficient buffering in the JVM. It is recommended to unzip first (as mentioned above)
# MAGIC 
# MAGIC This project serves as an example to implement Apache Spark custom Structured Streaming Sources. 
# MAGIC 
# MAGIC This project is accompanied by [Spark Custom Stream Sources](https://hackernoon.com/spark-custom-stream-sources-ec360b8ae240)

# COMMAND ----------

# MAGIC %md
# MAGIC Next steps
# MAGIC * Add publicly available source data; make publishable
# MAGIC * (?) Bronze-silver-gold layer; optionally analysis 

# COMMAND ----------


