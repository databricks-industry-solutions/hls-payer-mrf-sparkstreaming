package com.databricks.labs.sparkstreaming.jsonmrf

import collection.mutable.{Stack, ListBuffer}
import org.apache.spark.unsafe.types.UTF8String

/*
 * Represents an offset for Spark to consume. This can be made up of one or more Byte Arrays
 */
case class SparkChunk(chunk: JsonChunk*){
  def toSeq(): UTF8String = {
    UTF8String.fromBytes( chunk.map(_.toSlice).reduce((x,y) => x ++y) )
  }
}

/*
 * Represents a buffered read stream byte array 
 */
case class JsonChunk(
  buf: Array[Byte],
  size: Int,
  startIndex: Int,
  endIndex: Int
){
  def toSlice(): Array[Byte] ={
    buf.slice(startIndex, endIndex)
  }
}
