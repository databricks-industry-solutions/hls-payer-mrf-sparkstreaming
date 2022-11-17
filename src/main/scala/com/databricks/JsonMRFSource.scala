package com.databricks.labs.sparkstreaming.jsonmrf

import collection.mutable.{Stack, ListBuffer}
import java.util.zip.GZIPInputStream
import java.io.{InputStreamReader, BufferedInputStream}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, Row, Dataset}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.unsafe.types.UTF8String
import org.apache.hadoop.fs.{FileSystem, Path}

class JsonMRFSource (sqlContext: SQLContext, options: Map[String, String]) extends Source {
  private var offset: LongOffset = LongOffset(-1)
  private var batches = ListBuffer.empty[(SparkChunk, Long)]
  private val BufferSize = 268435456 //256MB

  private val hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
  private val fs = FileSystem.get(hadoopConf)
  private val fileStream = fs.open(new Path(options.get("path").get))
  private val inStream = options.get("path").get match {
    case ext if ext.endsWith("gz") =>   new BufferedInputStream(new GZIPInputStream(fileStream), BufferSize) //128MB buffer
    case ext if ext.endsWith("json") => new BufferedInputStream(fileStream, BufferSize) //128MB buffer
    case _ => throw new Exception("codec for file extension not implemented yet")
  }
  override def schema: StructType = JsonMRFSource.schema
  override def getOffset: Option[Offset] = this.synchronized {
    if (offset == -1) None else Some(offset)
  }

 
  val reader = new Thread(){
    /*
     * Recursive attempt at parsing. Return value is umatched array Bytes that need to be merged with next buffer read
     */
    def parse(bytesRead: Int, buffer: Array[Byte], startIndex: Int, headerKey: Option[String]): Option[Array[Byte]] = {
      println("DEBUG START: startIndex:" + startIndex + " - headerKey:" + headerKey)
      println("DEBUG END: ")

      headerKey match {
        case Some("provider_references") | Some("in_network") =>
          ByteParser.seekEndOfArray(buffer, startIndex, bytesRead)  match {
            case (x, ByteParser.EOB) => //array has been cut between bytes
              this.synchronized {
                offset = offset + 1
                batches.append( (new SparkChunk(Seq(Array('['.toByte), buffer.slice(startIndex, x+1), Array(']'.toByte))), offset.offset ) )
              }
              return Some(buffer.slice(x+1, bytesRead)) //return leftovers
            case (x,y) => //full record found within a byteArray. Y = outer array end, x = last element inside array end
              this.synchronized {
                offset = offset + 1
                batches.append( (new SparkChunk(Seq(Array('['.toByte), buffer.slice(startIndex, x+1), Array(']'.toByte))), offset.offset ) )
              }
              return parse(bytesRead - (y+1), buffer.slice(y+1, bytesRead), 0, None)
          }
        case _ => //indicates we are at a header level
          val arrayStartIndex = ByteParser.parseUntilArrayLeft(buffer, bytesRead) //the outermost [ wrapping a header array
          arrayStartIndex match {
            case ByteParser.EOB => //no more arrays, is any more header items remaining?
              if ( ByteParser.findByteRight(buffer, ByteParser.Colon, bytesRead-1, bytesRead) > 0  ){
                var header = buffer.slice(startIndex, bytesRead-1)
                if ( header(0).toInt == ByteParser.Comma ) header(0) = ByteParser.OpenB.toByte
                this.synchronized{
                  offset = offset + 1
                  batches.append( ( new SparkChunk(Seq(header)),offset.offset ) )
                }
              }
              return None
            case _ =>
              val arrayKeyTuple = ByteParser.searchKeyRight(buffer, bytesRead, arrayStartIndex)
              arrayKeyTuple match {
                case (None, _) => ??? //this is where we cannot find the key in our buffer... edge case will not do this for now
                case (Some(x), _) =>
                  val headerEnding = ByteParser.findByteRight(buffer, ByteParser.Comma, arrayKeyTuple._2, bytesRead)
                  if (headerEnding != -1) { //header before array
                    var header = buffer.slice(startIndex, headerEnding + 1)
                    //this header object is not at the beginning of the file
                    if ( header(0).toInt == ByteParser.Comma ) header(0) = ByteParser.OpenB.toByte
                    if ( header(headerEnding).toInt != ByteParser.CloseB) header(headerEnding) = ByteParser.CloseB.toByte
                    this.synchronized{
                      offset = offset + 1
                      batches.append( ( new SparkChunk(Seq(header)),offset.offset ) )
                    }
                  }
                  val innerArrayIndex = ByteParser.findByteLeft(buffer, ByteParser.OpenB, arrayStartIndex+1, bytesRead).min(ByteParser.findByteLeft(buffer, ByteParser.OpenL, arrayStartIndex+1, bytesRead)) //can this be -1 and therefore be wrong?
                  return parse(bytesRead,buffer, innerArrayIndex, Some(x))//no header between arrays
                case _ => throw new Exception("Should not get here")
              }
          }
      }
    }
    /*
     *
     */
    override def run(){
      val buffer = new Array[Byte](BufferSize)
      val internalBufSize = inStream.available
      val bytesRead = inStream.read(buffer,0, BufferSize)
      val rv = parse(bytesRead, buffer, 0, None)
      

    }
    def runs(){

      val buffer = new Array[Byte](BufferSize)
      val internalBufSize = inStream.available //usually this is 2GB
      val bytesRead = inStream.read(buffer,0, BufferSize)


      //Read and parse Header Json, create a smaller buffer to copy this data into
      val arrayStartIndex = ByteParser.parseUntilArrayLeft(buffer, bytesRead)
      val arrayKeyTuple = ByteParser.searchKeyRight(buffer, bytesRead, arrayStartIndex) //returns array key and the beginning location of the key
      val headerEnding = ByteParser.findByteRight(buffer, ByteParser.Comma, arrayKeyTuple._2, bytesRead)
      var header = buffer.slice(0, headerEnding + 1) 
      header(headerEnding) = ByteParser.CloseB.toByte

      this.synchronized{
        offset = offset + 1 
        batches.append( ( new SparkChunk(Seq(header)),offset.offset ) )
      }

      //Read Array Json, more headers, etc
      var startIndex = ByteParser.findByteLeft(buffer, ByteParser.OpenB, headerEnding+1, bytesRead)
      var validCheckpoint = -1
      var endIndex = -1
      var prevChunk = Seq[Array[Byte]]()

      arrayKeyTuple._1 match {
        case Some("provider_references") =>
          ByteParser.seekEndOfArray(buffer, startIndex, bytesRead)  match {
            case (_, ByteParser.EOB) => ??? //array cut in half
            case (x,y) => //full record found within a byteArray
              this.synchronized {
                offset = offset + 1
                batches.append( (new SparkChunk(Seq(buffer.slice(arrayStartIndex, y+1))), offset.offset ) )
              }
              prevChunk :+ buffer.slice(x+1, bytesRead)
          }

        case Some("in_network") => ???
        case _ => ???
      }
    }
  }

  reader.start()

  //Not sure why convert method was removed from LongOffset
  //Old version https://jar-download.com/artifacts/org.apache.spark/spark-sql_2.12/2.4.0/source-code/org/apache/spark/sql/execution/streaming/LongOffset.scala
  //New version https://jar-download.com/artifacts/org.apache.spark/spark-sql_2.12/3.2.1/source-code/org/apache/spark/sql/execution/streaming/LongOffset.scala
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = this.synchronized {

    val s = start.flatMap({ off =>
      off match {
        case lo: LongOffset => Some(lo)
        case _ => None
      }
    }).getOrElse(LongOffset(-1)).offset+1

    val e = (end match {
      case lo: LongOffset => lo
      case _ => LongOffset(-1)
    }).offset+1

    println(s"generating batch range $start ; $end")

    val data = batches
      .par
      .filter { case (_, idx) => idx >= s && idx <= e}
      .map{  case(v, _) => InternalRow(v.toSeq) }
      .seq

    val plan = new LocalRelation(Seq(AttributeReference("data", StringType)()), data, isStreaming=true)
    val qe = sqlContext.sparkSession.sessionState.executePlan(plan)
    qe.assertAnalyzed()

    new Dataset(sqlContext.sparkSession, plan, RowEncoder(qe.analyzed.schema)).toDF
  }

  override def stop(): Unit = reader.stop

  override def commit(end: Offset): Unit = this.synchronized {
    val committed = (end match {
      case lo: LongOffset => lo
      case _ => LongOffset(-1) 
    }).offset + 1
    val toKeep = batches.filter { case (_, idx) => idx > committed }

    println(s"after clean size ${toKeep.length}")
    println(s"deleted: ${batches.size - toKeep.size}")

    batches = toKeep

  }
}

object JsonMRFSource {
  lazy val schema = StructType(List(StructField("json_payload", StringType))) //we're defining a generic string type for our JSON payloads
}
