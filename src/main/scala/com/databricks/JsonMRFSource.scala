package com.databricks.labs.sparkstreaming.jsonmrf

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import collection.mutable.{Stack, ListBuffer}
import java.util.zip.GZIPInputStream
import java.io.{InputStreamReader, BufferedInputStream}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.unsafe.types.UTF8String
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SerializableWritable


/*
 * Represents a streaming source of a json Payer MRF In Network File
 *  map['path'] -> fqPath of the json resource
 *
 */
class JsonMRFSource (sqlContext: SQLContext, options: Map[String, String]) extends Source {
  var offset: LongOffset = LongOffset(-1)
  var batches = ListBuffer.empty[(JsonPartition, Long)] // (tuple of (tuple of file start offset, file end offset), spark offset)
  val BufferSize = 268435456 //256MB

  val hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
  val confBroadcast = sqlContext.sparkContext.broadcast(new SerializableWritable(hadoopConf))
  val fs = FileSystem.get(hadoopConf)
  val fileName = new Path(options.get("path").get)
  val fileStream = fs.open(fileName)
  val inStream = options.get("path").get match {
    case ext if ext.endsWith("gz") =>   new BufferedInputStream(new GZIPInputStream(fileStream), 8192) //Gzip compression testing
    case ext if ext.endsWith("json") => new BufferedInputStream(fileStream, BufferSize) //256MB buffer
    case _ => throw new Exception("codec for file extension not implemented yet")
  }

  override def schema: StructType = JsonMRFSource.schema
  override def getOffset: Option[Offset] = this.synchronized {
    if (offset == -1) None else Some(offset)
  }

  /*
   * Here we define a thread to run in the driver to iterate over the file and provide byte offsets for splitting
   *
   */
  private def reader = new Thread("Json File Reader") {
    setDaemon(true)
    /*
     * Recursive attempt at parsing. Return value is umatched array Bytes that need to be merged with next buffer read
     *  Return value is a tuple of (<leftover unprocessed bytes from array>, headerKey)
     *  Side Effect, update shared memory "batches" and "offset" with relevant information to a split
     */
    def parse(bytesRead: Int, buffer: Array[Byte], startIndex: Int, headerKey: Option[String], fileOffset: Long): (Option[Array[Byte]],  Option[String]) = {
      headerKey match {
        //We are starting processing in the middle of a header array
        case Some("provider_references") | Some("in_network") =>
          val endOfArray = ByteParser.seekEndOfArray(buffer, startIndex, bytesRead)
          endOfArray match { 
            case (ByteParser.EOB, ByteParser.EOB) =>  //what if there is no valid array split OR end of Array in our Byte String?
              return (Some(buffer), headerKey)
            case (-1, _) =>
              throw new Exception("Unable to find a cuttoff within a buffer") //Expect at minimum to find a split in buffersize
            case (x, ByteParser.EOB) => //Array has not ended, but x is a valid offset to save
              this.synchronized {
                offset = offset + 1
                batches.append(( new JsonPartition(startIndex + fileOffset, x+fileOffset, headerKey.get), offset.offset ) )
              }
              //make sure the next start point is either a { or [
              val i = ByteParser.skipWhiteSpaceAndCommaLeft(buffer, x+1, bytesRead)
              return (Some(buffer.slice(i, bytesRead)),headerKey) //return leftovers
            case (x,y) => //full recrd found within a byteArray. Y = outer array end, x = last element inside array end
              this.synchronized {
                offset = offset + 1
                batches.append(( new JsonPartition(startIndex + fileOffset, x + fileOffset, headerKey.get), offset.offset ))
              }
              return parse(bytesRead, buffer, (y+1), None, fileOffset)
          }
        case _ => //indicates we are at a header level
          val arrayStartIndex = ByteParser.parseUntilArrayLeft(buffer, bytesRead, startIndex) //TODO should this be startIndex? the outermost [ wrapping a header array
          arrayStartIndex match {
            case ByteParser.EOB => //no more arrays, is any more header items remaining?
              if ( ByteParser.findByteRight(buffer, ByteParser.Colon, bytesRead-1, bytesRead) > 0  ){
                this.synchronized{
                  offset = offset + 1
                  batches.append(( new JsonPartition(startIndex + fileOffset, bytesRead-1 + fileOffset),offset.offset ))
                }
              }
              return (None, None)
            case _ =>
              val arrayKeyTuple = ByteParser.searchKeyRight(buffer, bytesRead, arrayStartIndex)
              arrayKeyTuple match {
                case (None, _) =>
                  ??? //this is where we cannot find the key in our buffer... edge case will not implement
                case (Some(x), _) => 
                  val headerEnding = ByteParser.findByteRight(buffer, ByteParser.Comma, arrayKeyTuple._2, bytesRead)
                  if (headerEnding != -1) { //header before array
                    this.synchronized{
                      offset = offset + 1
                      batches.append(( new JsonPartition(startIndex + fileOffset, headerEnding+1 + fileOffset) ,offset.offset ) )
                    }
                  }
                  val innerArrayIndex = ByteParser.findByteArrayBeginningLeft(buffer,arrayStartIndex+1, bytesRead)
                  return parse(bytesRead,buffer, innerArrayIndex, Some(x), fileOffset) //No header between arrays
                case _ =>
                  ??? 
              }
          }
        }
    }
    /*
     * Loop over the file, keeping track of fileOffsets until reaching EOF 
     */
    override def run(): Unit = {
      val buffer = new Array[Byte](BufferSize)
      val internalBufSize = inStream.available
      var bytesRead = 0
      var totalBytesRead = 0L
      var bytesRemaining = 0
      var bufferRemaining = Array[Byte]()
      var headerKey = None: Option[String]
      var parsingByteSize = 0
      var parsingBuffer = Array[Byte]()
      while ( {bytesRead = inStream.read(buffer,0, BufferSize); bytesRead} != -1 ) {

        totalBytesRead += bytesRead
        println("Total bytes read so far: " + totalBytesRead)

        //leftovers bytes unmatched (if any)
        parsingByteSize = bytesRead + bytesRemaining
        if ( bytesRemaining > 0 ) {
          parsingBuffer = bufferRemaining ++ buffer
        }
        else parsingBuffer = buffer

        //Parse byte array and save off the return bytes which are not part of an offset block yet
        var rv = parse(parsingByteSize, parsingBuffer, 0, headerKey, totalBytesRead - bytesRead)
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
      println("Resources closed")
    }
  }
  reader.start()

  //Not sure why convert method was removed from LongOffset
  //Old version https://jar-download.com/artifacts/org.apache.spark/spark-sql_2.12/2.4.0/source-code/org/apache/spark/sql/execution/streaming/LongOffset.scala
  //New version https://jar-download.com/artifacts/org.apache.spark/spark-sql_2.12/3.2.1/source-code/org/apache/spark/sql/execution/streaming/LongOffset.scala
  override def getBatch(start: Option[Offset], end: Offset): DataFrame =  this.synchronized {
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

    println(s"generating spark batch range $start ; $end")

    /*
     * Capture valid byte offsets to be committed
     */
    val catalystRows = new JsonMRFRDD(
      sqlContext.sparkContext,
      confBroadcast,
      batches.par.filter{ case (_, idx) => idx >= s && idx <= e}.zipWithIndex.map({ case (v, idx2) => new JsonPartition(v._1.start, v._1.end, v._1.headerKey, idx2)}).toArray,
      fileName
    )
    /*
     * Give the Spark an execution plan on turning an array of offsets into an RDD of data from those offsets
     *  e.g. fileOffset (start, end)  -> Row(..data...)
     *  Partition[offset1, offset2...) -> RDD(Row1, Row2...)
     */
    val logicalPlan = LogicalRDD(
      JsonMRFSource.schemaAttributes,
      catalystRows,
      isStreaming = true)(sqlContext.sparkSession)

    val qe = sqlContext.sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new org.apache.spark.sql.Dataset(sqlContext.sparkSession, logicalPlan, RowEncoder(qe.analyzed.schema))
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
  lazy val schema = StructType(List(StructField("file_name",StringType), StructField("header_key", StringType), StructField("json_payload", StringType))) //we're defining a generic string type for our JSON payloads
  lazy val schemaAttributes = Seq(AttributeReference("file_name", StringType, nullable = false)(), AttributeReference("header_key", StringType, nullable=true)(), AttributeReference("json_payload", StringType, nullable = true)())
}
