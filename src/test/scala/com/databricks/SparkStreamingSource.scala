package com.databricks.labs.sparkstreaming.jsonmrf

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{InputStreamReader, BufferedInputStream}
import java.util.zip.GZIPInputStream


class SparkStreamingSource extends BaseTest with BeforeAndAfter{

  test("Streaming Query") {

    val df = spark
      .readStream
      .format("com.databricks.labs.sparkstreaming.jsonmrf.JsonMRFSourceProvider")
      .load("src/test/resources/in-network-rates-fee-for-service-single-plan-sample.json")

    val query = (
      df.writeStream
        .outputMode("append")
        .format("parquet")
        .queryName("The JSON splitter")
        .option("checkpointLocation", "src/test/resources/temp_ffs_sample_chkpoint_dir")
        .table("temp_ffs_sample")
    )

    assert(query.isActive)

  }

  test("Streaming Schema Metadata Results"){

  }

  test("Streaming Results RowCounts"){

  }

  after{
    spark.sql("drop table temp_ffs_sample")
    fs.remove("")
    fs.close
    spark.stop

  }
}
