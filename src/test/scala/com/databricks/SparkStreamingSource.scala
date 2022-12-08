package com.databricks.labs.sparkstreaming.jsonmrf

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

class SparkStreamingSource extends BaseTest with BeforeAndAfter{

  test("Streaming Query Tests") {
    val df = ( spark.readStream 
      .format("com.databricks.labs.sparkstreaming.jsonmrf.JsonMRFSourceProvider") 
      .load("src/test/resources/in-network-rates-fee-for-service-single-plan-sample.json")
    )
    val query = (
      df.writeStream
        .outputMode("append")
        .format("parquet")
        .queryName("The JSON splitter")
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

  test("Spark Streaming Results Format Tests"){
    val df = ( spark.readStream
      .format("com.databricks.labs.sparkstreaming.jsonmrf.JsonMRFSourceProvider")
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

  after{
    fs.delete(new Path("src/test/resources/temp_ffs_sample_rdd_chkpoint_dir"), true)
    fs.delete(new Path("src/test/resources/temp_ffs_sample_chkpoint_dir"), true)
    fs.delete(new Path("src/test/resources/temp_ffs_sample"), true)
    fs.delete(new Path("src/test/resources/temp_ffs_sample_rdd"), true)
    fs.close
    spark.stop
  }
}
