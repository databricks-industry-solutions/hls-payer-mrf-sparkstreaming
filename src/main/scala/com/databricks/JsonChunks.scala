package com.databricks.labs.sparkstreaming.jsonmrf


import com.google.common.io.ByteStreams
import org.apache.hadoop.conf.Configuration
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.hadoop.fs.{FileSystem, Path, FSDataInputStream}
import java.io.BufferedInputStream
import scala.util.Random
import org.apache.spark.SerializableWritable
import org.apache.spark.broadcast.Broadcast

case class JsonPartition(start: Long, end: Long,  headerKey: String = "", idx: Int = 0 ) extends Partition{
  override def index: Int = idx
}

/*
 * Represents an offset for Spark to consume. This can be made up of one or more Byte Arrays
 */
private class JsonMRFRDD(
  sc: SparkContext,
  partitions: Array[JsonPartition],
  fileName: Path)
    extends RDD[InternalRow](sc, Nil) {

  override def getPartitions: Array[Partition] = {
   partitions.indices.map { i =>
      new JsonPartition(partitions(i).start, partitions(i).end, partitions(i).headerKey, i).asInstanceOf[Partition]
    }.toArray
   }

  //Only ever returning one "row" with the iterator...
  //Maybe change this in the future to break apart the json object further into individual rows?
  override def compute(thePart: Partition, context: TaskContext): Iterator[InternalRow] =  {
    val in = JsonMRFRDD.fs.open(fileName)
    //Close out fis, bufferinputstream objects, etc
    val part = thePart.asInstanceOf[JsonPartition]
    in.seek(part.start)

    var buffer = new Array[Byte](( part.end - part.start + 1).toInt)
    ByteStreams.readFully(in, buffer)
    in.close
    //Make the header data valid JSON values
    if (part.headerKey == ""){
      //startByte and the endBytes should be '{' or '}' in a header
      if( buffer(ByteParser.skipWhiteSpaceLeft(buffer, 0, (part.end - part.start + 1).toInt)) != ByteParser.OpenB )
        buffer = Array('{'.toByte) ++ buffer
      if( buffer(ByteParser.skipWhiteSpaceRight(buffer, (part.end - part.start).toInt, (part.end - part.start + 1).toInt)) != ByteParser.CloseB)
        buffer = buffer  :+ '}'.toByte
    }
    else{
      //this is an array, make sure it starts and ends with brackets
      buffer = Array('['.toByte) ++ buffer ++ Array(']'.toByte)

    }
    Seq(InternalRow(UTF8String.fromString(fileName.getName), UTF8String.fromString(part.headerKey), UTF8String.fromBytes(buffer))).toIterator
  }
}


object JsonMRFRDD{
  val fs = FileSystem.get(new Configuration)
}
