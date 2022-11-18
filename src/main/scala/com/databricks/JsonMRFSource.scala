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
  var offset: LongOffset = LongOffset(-1)
  var batches = ListBuffer.empty[(SparkChunk, Long)]
  val BufferSize = 268435456 //256MB

  val hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
  val fs = FileSystem.get(hadoopConf)
  val fileStream = fs.open(new Path(options.get("path").get))
  val inStream = options.get("path").get match {
    case ext if ext.endsWith("gz") =>   new BufferedInputStream(new GZIPInputStream(fileStream), 8192) //Gzip compression testing
    case ext if ext.endsWith("json") => new BufferedInputStream(fileStream, BufferSize) //256MB buffer
    case _ => throw new Exception("codec for file extension not implemented yet")
  }
  override def schema: StructType = JsonMRFSource.schema
  override def getOffset: Option[Offset] = this.synchronized {
    if (offset == -1) None else Some(offset)
  }
 
  private def reader = new Thread("Json File Reader") {
    setDaemon(true)
    /*
     * Recursive attempt at parsing. Return value is umatched array Bytes that need to be merged with next buffer read
     *  Return value is a tuple of (<leftover unprocessed bytes from array>, headerKey)
     */
    def parse(bytesRead: Int, buffer: Array[Byte], startIndex: Int, headerKey: Option[String]): (Option[Array[Byte]],  Option[String]) = {
      headerKey match {
        case Some("provider_references") | Some("in_network") =>
          val endOfArray = ByteParser.seekEndOfArray(buffer, startIndex, bytesRead)
          endOfArray match { //TODO what if there is no valid endOf the Array in our Byte String?
            case (ByteParser.EOB, ByteParser.EOB) =>
              return (None, headerKey)
            case (-1, _) =>
              throw new Exception("Unable to find a cuttoff within a buffer") 
            case (x, ByteParser.EOB) => //array has been cut between bytes
              this.synchronized {
                offset = offset + 1
                batches.append( (new SparkChunk(Seq(Array('['.toByte), buffer.slice(startIndex, x+1), Array(']'.toByte))), offset.offset ) )
              }
              //make sure the next start point is either a { or [
              val i = ByteParser.skipWhiteSpaceAndCommaLeft(buffer, x+1, bytesRead)
              return (Some(buffer.slice(i, bytesRead)),headerKey) //return leftovers
            case (x,y) => //full recrd found within a byteArray. Y = outer array end, x = last element inside array end
              this.synchronized {
                offset = offset + 1
                batches.append( (new SparkChunk(Seq(Array('['.toByte), buffer.slice(startIndex, x+1), Array(']'.toByte))), offset.offset ) )
              }
              return parse(bytesRead - (y+1), buffer.slice(y+1, bytesRead), 0, None)
          }
        case _ => //indicates we are at a header level
          val arrayStartIndex = ByteParser.parseUntilArrayLeft(buffer, bytesRead) //TODO should this be startIndex? the outermost [ wrapping a header array
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
              return (None, None)
            case _ =>
              val arrayKeyTuple = ByteParser.searchKeyRight(buffer, bytesRead, arrayStartIndex)
              arrayKeyTuple match {
                case (None, _) => ??? //this is where we cannot find the key in our buffer... edge case will not implement
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
                  val innerArrayIndex = ByteParser.findByteArrayBeginningLeft(buffer,arrayStartIndex+1, bytesRead)
                  return parse(bytesRead,buffer, innerArrayIndex, Some(x))//no header between arrays
                case _ => throw new Exception("Should not get here")
              }
          }
        }
    }
    /*
     *
     */
    override def run(): Unit = {
      val buffer = new Array[Byte](BufferSize)
      val internalBufSize = inStream.available
      println("Internal buffer size: " + internalBufSize)
      var bytesRead = 0
      var totalBytesRead = 0L
      var bytesRemaining = 0
      var bufferRemaining = Array[Byte]()
      var headerKey = None: Option[String]
      var parsingByteSize = 0
      var parsingBuffer = Array[Byte]()
      while ( {bytesRead = inStream.read(buffer,0, BufferSize); bytesRead} != -1 ) {
        //read in data
        totalBytesRead += bytesRead

        println("Total bytes read so far: " + totalBytesRead)
        //copy leftovers if any
        parsingByteSize = bytesRead + bytesRemaining
        if ( bytesRemaining > 0 ) {
          parsingBuffer = bufferRemaining ++ buffer
        }
        else parsingBuffer = buffer

        //Parse and get leftovers
        var rv = parse(parsingByteSize, parsingBuffer, 0, headerKey)
        rv._1 match {
          case None =>
            bytesRemaining = 0
            bufferRemaining = Array[Byte]()
          case Some(x) =>
            bytesRemaining = x.size
            bufferRemaining = x
        }
        headerKey = rv._2
      }

      //Close out resources
      println("Sleeping prior to closing resources")
      Thread.sleep(10000)
      inStream.close
      fileStream.close
      fs.close
      println("Resources closed")
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
    }).offset
    val toKeep = batches.filter { case (_, idx) => idx > committed }

    println(s"the deleted offset:" + committed)
    println(s"after clean size ${toKeep.length}")
    println(s"deleted: ${batches.size - toKeep.size}")

    batches = toKeep
  }
}

object JsonMRFSource {
  lazy val schema = StructType(List(StructField("json_payload", StringType))) //we're defining a generic string type for our JSON payloads
}
