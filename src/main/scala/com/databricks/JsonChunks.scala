package com.databricks.labs.sparkstreaming.jsonmrf

import collection.mutable.{Stack, ListBuffer}
import org.apache.spark.unsafe.types.UTF8String

/*
 * Represents an offset for Spark to consume. This can be made up of one or more Byte Arrays
 */
case class SparkChunk(chunk: Seq[Array[Byte]]){
  def toSeq(): UTF8String = {
    UTF8String.fromBytes( chunk.reduce((x,y) => x ++y ) )
  }
}

/*
 * Represents a buffered read stream byte array 

case class JsonChunk(
  buf: Array[Byte],
  size: Int,
  startIndex: Int,
  endIndex: Int
){
  def toSlice(): Array[Byte] ={
    buf.slice(startIndex, endIndex)
  }

  /*
   * When get cut in half copy into a new array 
   */
  def getLeftoversHead(): JsonChunk = {
    return new JsonChunk(buf.slice(0, startIndex), startIndex + 1, 0, startIndex)
  }

  /*
   * When records get cut in half copy into new array 
   */
  def getLeftoversTail(): JsonChunk = {
    return new JsonChunk(buf.slice(endIndex, size - 1), size - endIndex -1,endIndex, size-endIndex-1)
  }
}
 */
