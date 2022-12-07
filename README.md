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

``` bash
#download file from Datatbricks notebook

dbfs_file_download_location=/dbfs/user/hive/warehouse/payer_transparency.db/raw_files/in-network-rates-bundle-single-plan-sample.json
wget https://github.com/CMSgov/price-transparency-guide/blob/master/examples/in-network-rates/in-network-rates-bundle-single-plan-sample.json -O $dbfs_file_download_location

#unzip if extension is .gz
#gunzip -cd $dbfs_file_download_location > ...
```

```python
#parse the file using pyspark or scala spark
dbfs_file_download_location="/dbfs/user/hive/warehouse/payer_transparency.db/raw_files/in-network-rates-bundle-single-plan-sample.json"
target_table="hls_payer_transparency.in_network_rates"

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
while(query.isActive()):
    time.sleep(10) //sleep for 10 seconds

print("Query finished executing")
``` 

## Sample Data Output

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
target_table = "hls_payer_transparency.in_network_rates_network_array"
df = spark.sql("select json_payload from hls_dev_payer_transparency.payer_transparency_ingest_round2 where header_key='in_network').rdd.repartition(20)

spark.read \
  .json(df.rdd.map(lambda x: x[0].replace('\n', '\n'))) \
  .write \
  .mode("overwrite") \
  .saveAsTable(target_table)

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

