package com.databricks.labs.sparkstreaming.jsonmrf

import org.apache.hadoop.fs.Path
import org.scalatest._
import play.api.libs.json.Json

import scala.collection.mutable

class SparkStreamingSource extends BaseTest with BeforeAndAfter{

  test("Streaming Query Tests") {
    val df = ( spark.readStream 
      .format("payer-mrf")
      .load("src/test/resources/in-network-rates-fee-for-service-single-plan-sample.json")
    )
    val query = (
      df.writeStream
        .outputMode("append")
        .format("parquet")
        .queryName("The Json splitter")
        .option("checkpointLocation", "src/test/resources/temp_ffs_sample_chkpoint_dir")
        .start("src/test/resources/temp_ffs_sample")
    )
    Thread.sleep(5 * 1000)
    assert(query.isActive)
    query.processAllAvailable
    query.stop
    assert(!query.isActive)

    val resultDF = spark.read.format("parquet").load("src/test/resources/temp_ffs_sample")
    assert(resultDF.filter(resultDF("header_key") === "provider_references").count >= 1)
    assert(resultDF.filter(resultDF("header_key") === "in_network").count >= 1)
    assert(resultDF.filter(resultDF("header_key") === "").count >= 1)
  }

  test("Results are all valid JSON objects"){
    val df = ( spark.readStream
      .format("payer-mrf")
      .load("src/test/resources/in-network-rates-fee-for-service-single-plan-sample.json")
    )

    val query = (
      df.writeStream
        .outputMode("append")
        .format("parquet")
        .queryName("The JSON splitter")
        .option("checkpointLocation", "src/test/resources/temp_ffs_sample_json_chkpoint_dir")
        .start("src/test/resources/temp_ffs_sample_json")
    )
    Thread.sleep(5 * 1000)
    assert(query.isActive)
    query.processAllAvailable
    query.stop
    assert(!query.isActive)

    val resultDF = spark.read.format("parquet").load("src/test/resources/temp_ffs_sample_json")
    val jsonCollection = resultDF.select(resultDF("json_payload")).collect
    jsonCollection.map(x => Json.parse(x.getString(0)))
  }

  test("Spark Streaming Results Format Tests"){
    val df = ( spark.readStream
      .format("payer-mrf")
      .load("src/test/resources/in-network-rates-fee-for-service-single-plan-sample.json")
    )
    val query = (
      df.writeStream
        .outputMode("append")
        .format("parquet")
        .queryName("The JSON splitter")
        .option("checkpointLocation", "src/test/resources/temp_ffs_sample_rdd_chkpoint_dir")
        .start("src/test/resources/temp_ffs_sample_rdd")
    )
    Thread.sleep(5 * 1000)
    assert(query.isActive)
    query.processAllAvailable
    query.stop
    assert(!query.isActive)

    val resultDF = spark.read.format("parquet").load("src/test/resources/temp_ffs_sample_rdd")
    val rdd = resultDF.filter(resultDF("header_key") === "in_network").select(resultDF("json_payload")).rdd.map(row => row.getString(0))
    val jsonDF = spark.read.json(rdd)
    assert(!jsonDF.columns.contains("_corrupt_record"))
    assert(jsonDF.count >= 1)
    assert(jsonDF.select("billing_code_type").first.getString(0) == "CPT")
    assert(jsonDF.select("billing_code").first.getString(0) == "27447")
    assert(jsonDF.schema.filter(x => x.name == "negotiated_rates").size >= 1)
  }

  test("Streaming Query w/ nongzip json payload as an Array"){
    val df = ( spark.readStream
      .format("payer-mrf")
      .option("payloadAsArray", "true")
      //.load("src/test/resources/in-network-rates-fee-for-service-single-plan-sample.json")
      .load("/Users/srijit.nair/Downloads/2023-01-05_d9a9de82-1ee0-4823-829d-2f41bc4a32ed_Aetna-Life-Insurance-Company.json")
    )
    val query = (
      df.writeStream
        .outputMode("append")
        .format("parquet")
        .queryName("The JSON Array splitter")
        .option("checkpointLocation", "src/test/resources/temp_ffs_array_rdd_chkpoint_dir")
        .start("src/test/resources/temp_ffs_array_rdd")
    )
    Thread.sleep(5 * 1000)
    assert(query.isActive)
    query.processAllAvailable
    query.stop
    assert(!query.isActive)

    val resultDF = spark.read.format("parquet").load("src/test/resources/temp_ffs_array_rdd")
    assert(resultDF.count >= 1)

    val jsonCollection = resultDF.select(resultDF("json_payload")).collect

    //checking if arrays are getting created
    assert(jsonCollection(0).getAs[mutable.WrappedArray[String]](0).length >0)

    jsonCollection.map(row => row.getAs[Seq[String]](0).map(x => Json.parse(x))) //assert each element of array is a json object
  }

  test("Streaming Query w/ gzipped json payload as an Array"){
    val df = ( spark.readStream
      .format("payer-mrf")
      .option("payloadAsArray", "true")
      //.load("src/test/resources/in-network-rates-fee-for-service-single-plan-sample.json.gz")
      .load("/Users/srijit.nair/Downloads/2023-01-05_d9a9de82-1ee0-4823-829d-2f41bc4a32ed_Aetna-Life-Insurance-Company.json.gz")
      )
    val query = (
      df.writeStream
        .outputMode("append")
        .format("parquet")
        .queryName("The JSON Array splitter")
        .option("checkpointLocation", "src/test/resources/temp_ffs_array_rdd_chkpoint_dir")
        .start("src/test/resources/temp_ffs_array_rdd")
      )
    Thread.sleep(5 * 1000)
    assert(query.isActive)
    query.processAllAvailable
    query.stop
    assert(!query.isActive)

    val resultDF = spark.read.format("parquet").load("src/test/resources/temp_ffs_array_rdd")
    assert(resultDF.count >= 1)

    val jsonCollection = resultDF.select(resultDF("json_payload")).collect

    //checking if arrays are getting created
    assert(jsonCollection(0).getAs[mutable.WrappedArray[String]](0).length >0)

    jsonCollection.map(row => row.getAs[Seq[String]](0).map(x => Json.parse(x) )) //assert each element of array is a json object
  }

  after{
    fs.delete(new Path("src/test/resources/temp_ffs_sample_rdd_chkpoint_dir"), true)
    fs.delete(new Path("src/test/resources/temp_ffs_sample_chkpoint_dir"), true)
    fs.delete(new Path("src/test/resources/temp_ffs_sample_json_chkpoint_dir"), true)
    fs.delete(new Path("src/test/resources/temp_ffs_array_rdd_chkpoint_dir"), true)
    fs.delete(new Path("src/test/resources/temp_ffs_sample"), true)
    fs.delete(new Path("src/test/resources/temp_ffs_sample_rdd"), true)
    fs.delete(new Path("src/test/resources/temp_ffs_sample_json"), true)
    fs.delete(new Path("src/test/resources/temp_ffs_array_json"), true)
    fs.delete(new Path("src/test/resources/temp_ffs_array_rdd"), true)
    fs.close
    spark.stop
  }
}
