package com.databricks.labs.sparkstreaming.jsonmrf

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{InputStreamReader, BufferedInputStream}
import java.util.zip.GZIPInputStream


abstract class StreamingTest extends AnyFunSuite{

  def spark: SparkSession = {
    val session = SparkSession.builder()
      .master("local[2]")
      .config("spark.driver.bindAddress","127.0.0.1") //Explicitly state this for Spark3.2.1
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    SparkSession.setActiveSession(session)
    session
  }

  def fs: FileSystem = {
    FileSystem.get(spark.sqlContext.sparkSession.sessionState.newHadoopConf)
  }


  def gzResource: BufferedInputStream = {
    new BufferedInputStream(new GZIPInputStream(fs.open(new Path("src/resources/test/test.json"))))
  }

  def jsonResource: BufferedInputStream = {
     new BufferedInputStream(fs.open(new Path("src/resources/test/test.json.gz")))
  }
}
