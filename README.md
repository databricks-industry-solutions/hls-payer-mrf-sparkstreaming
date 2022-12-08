# SparkStreamSources
Spark Custom Stream Source for splitting large Payer MRF json formats



## Use Case 


CMS Schemas for MRF are built using a single json object which spark by default cannot split. Reading these large files often results in Out of Memory errors. This parser serves to transform any array objects under the header into multiple splittable lists that can be parsed. See sample schemas here ->  https://github.com/CMSgov/price-transparency-guide/tree/master/schemas




## Recommended Spark Settings

``` python
spark.executor.cores 2
spark.executor.memory 5g
spark.executor.defaultJavaOptions -Xmx12g -Xms4g
spark.executor.instances 4
spark.driver.defaultJavaOptions -Xmx16g -Xms8g
spark.driver.cores 3
spark.driver.memory 8g
spark.rpc.message.maxSize 1024
```

## Running

### Download a sample file in Databricks notebook to DBFS
``` bash
dbfs_file_download_location=/dbfs/user/hive/warehouse/payer_transparency.db/raw_files/in-network-rates-bundle-single-plan-sample.json

wget https://github.com/CMSgov/price-transparency-guide/blob/master/examples/in-network-rates/in-network-rates-bundle-single-plan-sample.json -O $dbfs_file_download_location

#(recommended) unzip if extension is .gz
#gunzip -cd $dbfs_file_download_location > /dbfs/user/hive...
```
### Stream the downloaded sample to target delta table

```python
#parse the file using pyspark or scala spark
dbfs_file_download_location="dbfs:/user/hive/warehouse/payer_transparency.db/raw_files/in-network-rates-bundle-single-plan-sample.json"
target_table="hls_payer_transparency.in_network_rates_sample"

df = spark.readStream \
    .format("com.databricks.labs.sparkstreaming.jsonmrf.JsonMRFSourceProvider") \
    .load(dbfs_file_download_location)

query = (
df.writeStream 
    .outputMode("append") 
    .format("delta")
    .queryName("spark-readstream-in-network-rates-bundle-example")
    .option("checkpointLocation", dbfs_file_download_location + "_chkpoint_dir")
    .table(target_table)
   )
   
import time
lastBatch = -2 #Spark batches start at -1
print("Sleeping for 30 seconds and then checking if query is still running...")
time.sleep(30)
while lastBatch != query.lastProgress.get('batchId'):
  lastBatch =  query.lastProgress.get('batchId')
  time.sleep(15) #sleep for 15 second intervals

query.stop()    
print("Query finished")
``` 


## Sample Data Output from a larger payer

``` python
df = spark.table(target_table)
df.printSchema()

# df:pyspark.sql.dataframe.DataFrame
#  file_name:string
#  header_key:string
#  json_payload:string

spark.sql("select file_name, header_key, substr(json_payload, 1, 20) from " + target_table).show()
+--------------------+-------------------+---------------------------+
|           file_name|         header_key|substr(json_payload, 1, 20)|
+--------------------+-------------------+---------------------------+
|in-network-rates-...|provider_references|       {"provider_groups": |
|in-network-rates-...|         in_network|       {"negotiation_arrang|
|in-network-rates-...|         in_network|       {"negotiation_arrang|
|in-network-rates-...|         in_network|       {"negotiation_arrang|
|in-network-rates-...|         in_network|       {"negotiation_arrang|
|in-network-rates-...|                   |       {"reporting_entity":|

#building out a sample silver table with schema inference
df = spark.sql("select json_payload from " + target_table + " where header_key='in_network').rdd.repartition(20)

spark.read \
  .json(df.rdd.map(lambda x: x[0].replace('\n', '\n'))) \
  .write \
  .mode("overwrite") \
  .saveAsTable("hls_payer_transparency.in_network_rates_network_array")

spark.table(target_table).printSchema()
# billing_code:string
# billing_code_type:string
# billing_code_type_version:string
# description:string
# name:string
# negotiated_rates:array
#  element:struct
# ....

```

## Where to get MRF Files? 

Here are some of the USA's larger payers and landing page
1. UHG https://transparency-in-coverage.uhc.com/
2. Anthem https://www.anthem.com/machine-readable-file/search/
3. Cigna https://www.cigna.com/legal/compliance/machine-readable-files
4. Aetna https://health1.aetna.com/app/public/#/one/insurerCode=AETNACVS_I&brandCode=ALICSI/machine-readable-transparency-in-coverage?reportingEntityType=Third%20Party%20Administrator_6644&lock=true
5. Humana https://developers.humana.com/syntheticdata/Resource/PCTFilesList?fileType=innetwork


## Meeting CMS 2023, 2024 Comparison Mandates
Check out the demo notebook, "01_payer_mrf_demo.py" to ingest, split, and create a simple data model for meeting CMS mandates from an in-network file

## Contributing to the package

### building
```scala
sbt package
```
### testing
```scala
sbt test
```

## Speed 

On a local spark cluster with xmx8g processing around 5-7GB per minute. Note of caution, this program depends on buffering. Some forms of .gz extension do not enable efficient buffering in the JVM. It is recommended to gunzip -d the file first prior to running

