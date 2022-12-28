# Custom Spark Streaming Source - Payer MRF

## Use Case 


CMS Schemas for MRF are built using a single json object which spark by default cannot split. Reading these large files often results in Out of Memory errors. This parser serves to transform any array objects under the header into multiple splittable lists that can be parsed. See sample schemas here ->  https://github.com/CMSgov/price-transparency-guide/tree/master/schemas



## Running

### Attaching Library to Cluster 

Note: attach jar package to your **Spark 3.2.1, Scala 2.12** cluster from the "latest" release here

### Getting Sample Data
``` bash
dbfs_file_download_location=/dbfs/user/hive/warehouse/payer_transparency.db/raw_files/in-network-rates-bundle-single-plan-sample.json

wget https://github.com/CMSgov/price-transparency-guide/blob/master/examples/in-network-rates/in-network-rates-bundle-single-plan-sample.json -O $dbfs_file_download_location

#(recommended) unzip if extension is .gz
#gunzip -cd $dbfs_file_download_location > /dbfs/user/hive...
```
### Running the code

```python
#parse the file using pyspark or scala spark
dbfs_file_download_location="dbfs:/user/hive/warehouse/payer_transparency.db/raw_files/in-network-rates-bundle-single-plan-sample.json"
target_table="hls_payer_transparency.in_network_rates_sample"

df = spark.readStream \
    .format("com.databricks.labs.sparkstreaming.jsonmrf.JsonMRFSourceProvider") \
    .load(dbfs_file_download_location)
```

### Analyzing results

```python 
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
print("Sleeping for 60 seconds and then checking if query is still running...")
time.sleep(60)
while lastBatch != query.lastProgress.get('batchId'):
  lastBatch =  query.lastProgress.get('batchId')
  time.sleep(60) #sleep for 60 second intervals

query.stop()    
print("Query finished")
``` 


## Sample Data and Schema from larger file

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
|in-network-rates-...|provider_references|      [{"provider_groups": |
|in-network-rates-...|         in_network|      [{"negotiation_arrang|
|in-network-rates-...|         in_network|      [{"negotiation_arrang|
|in-network-rates-...|         in_network|      [{"negotiation_arrang|
|in-network-rates-...|         in_network|      [{"negotiation_arrang|
|in-network-rates-...|                   |       {"reporting_entity":|

#building out a sample silver table with schema inference
rdd = spark.sql("select json_payload from " + target_table + " where header_key='in_network').rdd.repartition(20)

spark.read \
  .json(rdd.map(lambda x: x[0].replace('\n', '\n'))) \
  .write \
  .mode("overwrite") \
  .saveAsTable("hls_payer_transparency.in_network_rates_network_array")

spark.table("hls_payer_transparency.in_network_rates_network_array").printSchema()
# billing_code:string
# billing_code_type:string
# billing_code_type_version:string
# description:string
# name:string
# negotiated_rates:array
#  element:struct
# ....

```

## Meeting CMS 2023, 2024 Comparison Mandates
Check out the demo notebook ***01_payer_mrf_demo.py*** to ingest, split, create a simple data model, and a lightweight query for complying with the CMS mandates for comparable prices.


## F.A.Q.

1. How fast can it parse? 

On a local spark cluster with xmx8g processing around 5-7GB per minute. Note of caution, this program depends on a few things which can cause varrying results. E.g. Some forms of .gz extension do not enable efficient buffering in the JVM. It is recommended to gunzip -d the file first prior to running

2. What is the recommended cluster settings? 

Having 2 executors and these Spark settings should be enough to get started. When running multiple files in parallel you'll want to increase the # of executors, and potentially increase the driver size.

``` python
spark.driver.cores 3
spark.driver.memory 8g
spark.rpc.message.maxSize 1024
```

3. Can I contribute to the package? 

Yes, package is released under Databricks License and built in SBT. 

#### building
```scala
sbt package
```
#### running tests
```scala
sbt test
```

4. Where to get Payer MRF Files? 

Here are some of the USA's larger payers and landing page
 - UHG https://transparency-in-coverage.uhc.com/
 - Anthem https://www.anthem.com/machine-readable-file/search/
 - Cigna https://www.cigna.com/legal/compliance/machine-readable-files
 - Aetna https://health1.aetna.com/app/public/#/one/insurerCode=AETNACVS_I&brandCode=ALICSI/machine-readable-transparency-in-coverage?reportingEntityType=Third%20Party%20Administrator_6644&lock=true
 - Humana https://developers.humana.com/syntheticdata/Resource/PCTFilesList?fileType=innetwork


