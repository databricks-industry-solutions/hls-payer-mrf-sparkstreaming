package com.databricks.labs.sparkstreaming.jsonmrf

import org.apache.hadoop.conf.Configuration
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.BufferedInputStream
import scala.util.Random

case class JsonPartition(start: Long, end: Long, idx: Int) extends Partition{
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
      new JsonPartition(partitions(i).start, partitions(i).end, i).asInstanceOf[Partition]
    }.toArray
  }

  //Only ever returning one "row" with the iterator...
  //Maybe change this in the future to break apart the json object further into individual rows?
  override def compute(thePart: Partition, context: TaskContext): Iterator[InternalRow] =  {
    //Close out fis, bufferinputstream objects, etc
    val fs = FileSystem.get(new Configuration)
    val in = fs.open(fileName)
    val part = thePart.asInstanceOf[JsonPartition]

    in.seek(part.start)
    val inStream = new BufferedInputStream(in)

    context.addTaskCompletionListener[Unit] { context =>  {
      inStream.close
      in.close()
    }}

    val bytes = Iterator.continually(inStream.read)
      .take( (part.end - part.start + 1).asInstanceOf[Int] ) //amount of bytes read should never be more than Int (~ < 3G)
      .map(_.toByte)
      .toArray
    Seq(InternalRow(UTF8String.fromBytes(bytes))).toIterator
  }
}


object JsonMRFRDD{

}
