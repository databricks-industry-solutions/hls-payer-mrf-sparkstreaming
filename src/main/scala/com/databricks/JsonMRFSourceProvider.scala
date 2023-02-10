package com.databricks.labs.sparkstreaming.jsonmrf

import com.google.common.io.ByteStreams
import java.io.{InputStreamReader, BufferedInputStream, BufferedOutputStream}
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.OutputMode
import java.util.zip.GZIPInputStream
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class JsonMRFSourceProvider extends StreamSourceProvider with DataSourceRegister with StreamSinkProvider {

  override def shortName(): String = "payer-mrf"

  override def sourceSchema(sqlContext: SQLContext,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]): (String, StructType) = {
    (shortName(), JsonMRFSource.getSchema({
      parameters.get("payloadAsArray") match {
        case Some("true") => true
        case _ => false
      }
    }))
  }

  override def createSource(sqlContext: SQLContext,
    metadataPath: String,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]): Source = {
    //parameters["path" -> "data/filename.json.gz"]
    //params["uncompressedPath" -> "data/filename.json"]
    val params = parameters.get("path").get match {

      case ext if ext.endsWith(".gz") =>
        val fs = FileSystem.get(sqlContext.sparkSession.sessionState.newHadoopConf())
        val inStream = new BufferedInputStream(new GZIPInputStream(fs.open(new Path(ext))), 268435456) //256MB
        val outStream = new BufferedOutputStream(fs.create(new Path(ext.dropRight(3)) ,true))
        ByteStreams.copy(inStream, outStream)
        inStream.close
        outStream.close
        parameters  + ("uncompressedPath" -> ext.dropRight(3))

      case ext if ext.endsWith(".json") =>
        parameters + ("uncompressedPath" -> ext)

      case _ => throw new Exception("codec for file extension not implemented yet")
    }
    new JsonMRFSource(sqlContext, params)
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


