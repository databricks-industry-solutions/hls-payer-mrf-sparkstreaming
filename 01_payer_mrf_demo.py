# Databricks notebook source
# MAGIC %md 
# MAGIC ## Example Workflow 
# MAGIC  Bronze (1) Download, (2) Unzip, (3) Stream  
# MAGIC  Silver (1) Curation ETL into desired Data Model  
# MAGIC  Gold (1) Serve Payer Transparency Comparison Tool (2023,2024) CMS Requirements

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze 

# COMMAND ----------

# MAGIC %sh
# MAGIC #(1) Download to DBFS storage
# MAGIC mkdir -p /dbfs/user/hive/warehouse/hls_dev_payer_transparency.db/raw_files/
# MAGIC wget -O /dbfs/user/hive/warehouse/hls_dev_payer_transparency.db/raw_files/2022-12-01_UMR--Inc-_Third-Party-Administrator_ENCORE-ENTERPRISES-AIRROSTI-DCI_TX-DALLAS-NON-EVALUATED-GAP_-ENC_NXBJ_in-network-rates.json.gz  https://uhc-tic-mrf.azureedge.net/public-mrf/2022-12-01/2022-12-01_UMR--Inc-_Third-Party-Administrator_ENCORE-ENTERPRISES-AIRROSTI-DCI_TX-DALLAS-NON-EVALUATED-GAP_-ENC_NXBJ_in-network-rates.json.gz

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh 
# MAGIC #(2) unzip in dbfs 
# MAGIC gunzip -cd /dbfs/user/hive/warehouse/hls_dev_payer_transparency.db/raw_files/2022-12-01_UMR--Inc-_Third-Party-Administrator_ENCORE-ENTERPRISES-AIRROSTI-DCI_TX-DALLAS-NON-EVALUATED-GAP_-ENC_NXBJ_in-network-rates.json.gz  > /dbfs/user/hive/warehouse/hls_dev_payer_transparency.db/raw_files/2022-12-01_UMR--Inc-_Third-Party-Administrator_ENCORE-ENTERPRISES-AIRROSTI-DCI_TX-DALLAS-NON-EVALUATED-GAP_-ENC_NXBJ_in-network-rates.json

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists hls_dev_payer_transparency;
# MAGIC set spark.sql.files.maxPartitionBytes=8388608
# MAGIC set spark.databricks.delta.optimizeWrite.enabled=true
# MAGIC set spark.databricks.delta.autoCompact.enabled=true

# COMMAND ----------

source_data = "dbfs:/user/hive/warehouse/hls_dev_payer_transparency.db/raw_files/2022-12-01_UMR--Inc-_Third-Party-Administrator_ENCORE-ENTERPRISES-AIRROSTI-DCI_TX-DALLAS-NON-EVALUATED-GAP_-ENC_NXBJ_in-network-rates.json"

# COMMAND ----------

# reinitialize tables to be created in this notebook
spark.sql("DROP TABLE IF EXISTS hls_dev_payer_transparency.payer_transparency_ingest")
spark.sql("""drop table if exists hls_dev_payer_transparency.payer_transparency_ingest_in_network""")
dbutils.fs.rm(source_data + "_checkpoint", True)
spark.sql("""drop table if exists hls_dev_payer_transparency.payer_transparency_in_network_provider_header""")
spark.sql("""drop table if exists hls_dev_payer_transparency.payer_transparency_in_network_provider_references""")
spark.sql("""drop table if exists hls_dev_payer_transparency.payer_transparency_in_network_in_network""")

# COMMAND ----------

df = spark.readStream.format("com.databricks.labs.sparkstreaming.jsonmrf.JsonMRFSourceProvider").load(source_data)
query = (
df.writeStream 
 .outputMode("append") 
 .format("delta") 
 .option("truncate", "false") 
 .option("checkpointLocation", source_data + "_checkpoint") 
 .table("hls_dev_payer_transparency.payer_transparency_ingest") 
)

# COMMAND ----------

import time
lastBatch = -2 #Spark batches start at -1
print("Sleeping for 60 seconds and then checking if query is still running...")
time.sleep(60)
while lastBatch != query.lastProgress.get('batchId'):
  lastBatch =  query.lastProgress.get('batchId')
  time.sleep(60) #sleep for 60 second intervals

query.stop()    
print("Query finished")

# COMMAND ----------

# MAGIC %md
# MAGIC Create tables from JSON structures for ETL development in SQL

# COMMAND ----------

provider_references_rdd = spark.sql("select json_payload from hls_dev_payer_transparency.payer_transparency_ingest where header_key='provider_references'").rdd.flatMap(lambda x:x)
header_rdd = spark.sql("select json_payload from hls_dev_payer_transparency.payer_transparency_ingest where header_key=''").rdd.flatMap(lambda x:x)
in_network_rdd = spark.sql("select json_payload from hls_dev_payer_transparency.payer_transparency_ingest where header_key='in_network'").rdd.flatMap(lambda x:x)

# COMMAND ----------

spark.read.json(header_rdd).write.mode("overwrite").saveAsTable("hls_dev_payer_transparency.payer_transparency_in_network_provider_header")
spark.read.json(provider_references_rdd).write.mode("overwrite").saveAsTable("hls_dev_payer_transparency.payer_transparency_in_network_provider_references")
spark.read.json(in_network_rdd).write.mode("overwrite").saveAsTable("hls_dev_payer_transparency.payer_transparency_in_network_in_network")

# COMMAND ----------

# MAGIC %md ### Silver 
# MAGIC ETL Curation to report off of 2023 mandate. Compare prices for a procedure (BILLING_CODE) within a provider group (TIN)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Provider References X Payer
# MAGIC drop table if exists hls_dev_payer_transparency.payer_transparency_in_network_provider_references_x_payer;
# MAGIC create table hls_dev_payer_transparency.payer_transparency_in_network_provider_references_x_payer
# MAGIC as
# MAGIC select   reporting_entity_name, reporting_entity_type, foo.provider_group_id, foo.group_array.npi, foo.group_array.tin
# MAGIC from 
# MAGIC (
# MAGIC select provider_group_id, explode(provider_groups) as group_array
# MAGIC from hls_dev_payer_transparency.payer_transparency_in_network_provider_references 
# MAGIC ) foo
# MAGIC inner join (select  reporting_entity_name, reporting_entity_type from  hls_dev_payer_transparency.payer_transparency_in_network_provider_header where reporting_entity_name is not null) entity
# MAGIC on 1=1 
# MAGIC ;
# MAGIC 
# MAGIC --Procedure Table
# MAGIC drop table if exists hls_dev_payer_transparency.payer_transparency_in_network_in_network_codes;
# MAGIC create table hls_dev_payer_transparency.payer_transparency_in_network_in_network_codes
# MAGIC as
# MAGIC select  uuid() as sk_in_network_id
# MAGIC ,n.billing_code
# MAGIC ,n.billing_code_type
# MAGIC ,n.billing_code_type_version
# MAGIC ,n.description
# MAGIC ,n.name
# MAGIC ,n.negotiation_arrangement
# MAGIC ,n.negotiated_rates
# MAGIC from hls_dev_payer_transparency.payer_transparency_in_network_in_network n
# MAGIC ;
# MAGIC 
# MAGIC -- M:M relationship between rates and network
# MAGIC drop table if exists hls_dev_payer_transparency.payer_transparency_in_network_in_network_rates;
# MAGIC create table hls_dev_payer_transparency.payer_transparency_in_network_in_network_rates
# MAGIC as
# MAGIC select  uuid() as sk_rate_id
# MAGIC ,foo.sk_in_network_id
# MAGIC ,foo.negotiated_rates_array
# MAGIC from
# MAGIC (
# MAGIC select sk_in_network_id, explode(c.negotiated_rates) as negotiated_rates_array
# MAGIC from hls_dev_payer_transparency.payer_transparency_in_network_in_network_codes c
# MAGIC ) foo
# MAGIC ;
# MAGIC 
# MAGIC --Procedure Price details
# MAGIC drop table if exists  hls_dev_payer_transparency.payer_transparency_in_network_in_network_rates_prices;
# MAGIC create table  hls_dev_payer_transparency.payer_transparency_in_network_in_network_rates_prices
# MAGIC as 
# MAGIC select sk_in_network_id, sk_rate_id, price.billing_class, price.billing_code_modifier, price.expiration_date, price.negotiated_rate, price.negotiated_type, price.service_code
# MAGIC from
# MAGIC (
# MAGIC select explode (negotiated_rates_array.negotiated_prices) as price, sk_rate_id, sk_in_network_id
# MAGIC from hls_dev_payer_transparency.payer_transparency_in_network_in_network_rates
# MAGIC ) foo
# MAGIC where price.negotiated_type = 'negotiated'
# MAGIC ; 
# MAGIC 
# MAGIC --Providers par in a price and procedure
# MAGIC drop table if exists  hls_dev_payer_transparency.payer_transparency_in_network_in_network_rates_par_providers;
# MAGIC create table  hls_dev_payer_transparency.payer_transparency_in_network_in_network_rates_par_providers
# MAGIC as
# MAGIC select provider_reference_id, sk_rate_id
# MAGIC from
# MAGIC (
# MAGIC select explode (negotiated_rates_array.provider_references) as provider_reference_id, sk_rate_id
# MAGIC from hls_dev_payer_transparency.payer_transparency_in_network_in_network_rates
# MAGIC ) foo
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold
# MAGIC 2023 Shoppable prices

# COMMAND ----------

dbutils.widgets.text("billing_code", "43283") # providing a default value that can be updated interactively
dbutils.widgets.text("tin_value", "161294447") # providing a default value that can be updated interactively

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT billing_code, description, billing_class, billing_code_modifier, service_code, negotiated_rate, npi, tin
# MAGIC FROM hls_dev_payer_transparency.payer_transparency_in_network_in_network_codes proc
# MAGIC inner join hls_dev_payer_transparency.payer_transparency_in_network_in_network_rates_prices price
# MAGIC 	on proc.sk_in_network_id = price.sk_in_network_id
# MAGIC inner join (hls_dev_payer_transparency.payer_transparency_in_network_in_network_rates_par_providers) provider_ref
# MAGIC 	on price.sk_rate_id = provider_ref.sk_rate_id
# MAGIC inner join (hls_dev_payer_transparency.payer_transparency_in_network_provider_references_x_payer) provider
# MAGIC 	on provider_ref.provider_reference_id = provider.provider_group_id
# MAGIC where billing_code = ${billing_code} -- picking  a random code
# MAGIC 	and negotiation_arrangement = 'ffs' and tin.value= ${tin_value} -- pick a random provider practice

# COMMAND ----------


