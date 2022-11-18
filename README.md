# SparkStreamSources
Spark Custom Stream Source and Sink for Payer MRF Use Case

## Running

``` python
df = spark.readStream
    .format("com.databricks.labs.sparkstreaming.jsonmrf.JsonMRFSourceProvider")
    .load("/Users/aaron.zavora//Downloads/umr-tpa-encore-in-network-rates.json")

df.writeStream
    .outputMode("append")
    .format("text")
    .queryName("umr-tpa-in-network-parsing")
    .option("checkpointLocation", "src/test/resources/chkpoint_dir")
    .start("src/test/resources/output")
``` 

## Use Case 

Schema definition that is parsed is the CMS in-network file. https://github.com/CMSgov/price-transparency-guide/tree/master/schemas/in-network-rates

## Data Sample

``` bash
more  src/test/resources/output/part-00000-a6af8cf3-6162-4d60-9acb-8933bac19b8b-c000.txt
>[{"negotiation_arrangement":"ffs","name":"BRONCHOSCOPY W/TRANSBRONCHIAL LUNG BX EACH LOBE","billi
...
>[{"negotiation_arrangement":"ffs","name":"ANESTHESIA EXTENSIVE SPINE & SPINAL CORD","bil

```

## Speed 

On a local Macbook with xmx8g running at 5-10GB per minute. Note of caution, this program depends on buffering. Some forms of .gz extension do not enable efficient buffering in the JVM. It is recommended to gunzip -d the file first prior to running

This project serves as an example to implement Apache Spark custom Structured Streaming Sources. 

This project is accompanied by [Spark Custom Stream Sources](https://hackernoon.com/spark-custom-stream-sources-ec360b8ae240)

