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
