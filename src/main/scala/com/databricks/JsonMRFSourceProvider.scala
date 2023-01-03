package com.databricks.labs.sparkstreaming.jsonmrf

import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}

class JsonMRFSourceProvider extends StreamSourceProvider with DataSourceRegister with StreamSinkProvider {

  override def shortName(): String = "payer-mrf"

  override def sourceSchema(sqlContext: SQLContext,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]): (String, StructType) = {
    (shortName(), JsonMRFSource.getSchema(parameters))
  }

  override def createSource(sqlContext: SQLContext,
    metadataPath: String,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]): Source = {
    //parameters["path" -> "data/filename.json"]
    new JsonMRFSource(sqlContext, parameters)
  }


  override def createSink(sqlContext: SQLContext,
    parameters: Map[String, String],
    partitionColumns: Seq[String],
    outputMode: OutputMode): Sink = new Sink {
    override def addBatch(batchId: Long, data: DataFrame): Unit = {
      println("BatchId: " + batchId)
        data.collect().foreach(println)
    }
  }
}


