package com.databricks.labs.sparkstreaming.jsonmrf

import collection.mutable.{Stack, ListBuffer}
import org.apache.spark.unsafe.types.UTF8String

case class SparkChunk(cur: JsonChunk, prev: JsonChunk){
  def toSeq(): UTF8String = {
    UTF8String.fromBytes(prev.toSlice ++ cur.toSlice)
  }
}

/*
 * Represents a buffered read stream
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
