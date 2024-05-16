package com.databricks.labs.sparkstreaming.jsonmrf


import com.google.common.io.ByteStreams
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{Partition, SparkContext, TaskContext}

import java.nio.charset.StandardCharsets

case class JsonPartition(start: Long, end: Long,  headerKey: String = "", idx: Int = 0 ) extends Partition{
  override def index: Int = idx
}

/*
 * Represents an offset for Spark to consume. This can be made up of one or more Byte Arrays
 */
private class JsonMRFRDD(
  sc: SparkContext,
  partitions: Array[JsonPartition],
  fileName: Path,
  payloadAsArray: Boolean = false)
    extends RDD[InternalRow](sc, Nil) {

  override def getPartitions: Array[Partition] = {
   partitions.indices.map { i =>
      new JsonPartition(partitions(i).start, partitions(i).end, partitions(i).headerKey, i).asInstanceOf[Partition]
    }.toArray
   }

  //Only ever returning one "row" with the iterator...
  //Maybe change this in the future to break apart the json object further into individual rows?
  override def compute(thePart: Partition, context: TaskContext): Iterator[InternalRow] =  {
    val in = JsonMRFRDD.fs(fileName).open(fileName)
    //Close out fis, bufferinputstream objects, etc
    val part = thePart.asInstanceOf[JsonPartition]
    in.seek(part.start)

    val buffersize = ( part.end - part.start + 1).toInt
    var buffer = new Array[Byte](buffersize)
    var isWhitespace = false

    ByteStreams.readFully(in, buffer)
    in.close
    //Make the header data valid JSON values
    if (part.headerKey == ""){
      //startByte and the endBytes should be '{' or '}' in a header
      val beginFromLeft = ByteParser.skipWhiteSpaceLeft(buffer, 0, buffersize)

      if (beginFromLeft < 0 ) {
        //If its all whitespace, no need to proceed further
        isWhitespace = true
      }else{
        if( buffer(beginFromLeft) != ByteParser.OpenB )
          buffer = Array('{'.toByte) ++ buffer

        val endFromRight = ByteParser.skipWhiteSpaceRight(buffer, buffersize-1, buffersize)
        if( buffer(endFromRight) != ByteParser.CloseB)
          buffer = buffer  :+ '}'.toByte

      }

    }
    if(!isWhitespace) {
      if (payloadAsArray) { //returns the json payload as an Array[String] instead of a String
        var arr = Array[UTF8String]()
        var start = 0
        var finish = 0
        do {
          finish = ByteParser.seekMatchingEndBracket(buffer, start, buffersize + 1)
          arr = arr :+ UTF8String.fromBytes(buffer.slice(start, finish + 1))
          start = ByteParser.arrayHasNext(buffer, finish, buffersize)
        } while (0 <= start && finish < buffersize)

        Seq(InternalRow(
          UTF8String.fromString(fileName.getName),
          UTF8String.fromString(part.headerKey),
          ArrayData.toArrayData(arr)
        )).toIterator
      }
      else {
        //return the json_payload as a String
        buffer = Array('['.toByte) ++ buffer ++ Array(']'.toByte)
        Seq(InternalRow(
          UTF8String.fromString(fileName.getName),
          UTF8String.fromString(part.headerKey),
          UTF8String.fromBytes(buffer)
        )).toIterator
      }
    }else{
      //if its all whitespace, just return an empty array
      Seq(InternalRow(
        UTF8String.fromString(fileName.getName),
        UTF8String.fromString(part.headerKey),
        {if (payloadAsArray) ArrayData.toArrayData(Array[UTF8String]()) else UTF8String.fromString("")})
      ).toIterator
    }
  }
}


object JsonMRFRDD{
  val fs = (p:Path) => p.getFileSystem(new Configuration)
}
