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
  private var batches = ListBuffer.empty[(UTF8String, Long)]
  private val hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
  private val fs = FileSystem.get(hadoopConf)
  private val fileStream = fs.open(new Path(options.get("path").get))
  private val inStream = options.get("path").get match {
    case ext if ext.endsWith("gz") =>   new BufferedInputStream(new GZIPInputStream(fileStream))
    case ext if ext.endsWith("json") => new BufferedInputStream(fileStream)
    case _ => throw new Exception("codec for file extension not implemented yet")
  }
  override def schema: StructType = JsonMRFSource.schema
  override def getOffset: Option[Offset] = this.synchronized {
    if (offset == -1) None else Some(offset)
  }
  
  val reader = new Thread(){
    override def run(){
      Stream.continually(inStream.read)
        .takeWhile(_ != -1)
        .foreach(JsonParser.parse)

      if (JsonParser.stack.length != 0) throw new Exception("Unexpected end of file")
      if (JsonParser.batch.length > 1) { //header info at the end of the file
        this.synchronized{
          offset = offset + 1
          batches.append((UTF8String.fromString("{" + JsonParser.batch.mkString),offset.offset))
        }
      }
      inStream.close
    }
  }

  /*
   * For now, assume valid in-network schema
   *  https://github.com/CMSgov/price-transparency-guide/tree/master/schemas/in-network-rates
   */
  object JsonParser {

    var prev = -1 //previouisly seen byte
    var isQuoted = false  //are we inbetween quotes? 
    var batch = ListBuffer.empty[Char] //the current batch we are building to write out   
    var stack = Stack.empty[Int]  //stack object to help determine how far nested the JSON is
 
    var isKey = true //if we are currently reading in a key value, starts out as true
    var headerKey = ListBuffer.empty[Char] //the last key we have read in the header


    //stack size is <= 1
    def parseHeader(i: Int): Unit = {
      i match {
        case CloseL =>
          this.synchronized{
            offset = offset + 1
            batches.append((UTF8String.fromString("{" + headerKey.mkString + ":" + batch.mkString),offset.offset))
          }
          batch = ListBuffer.empty[Char]
          headerKey = ListBuffer.empty[Char]

        case Colon => if ( !isQuoted ) {
          isKey = !isKey
          /*
           * Here, we see a key that needs to be seperated out into seperate records.
           *  This is the beginning of exiting the header
           */
          if (headerKey.mkString.toLowerCase == "provider_references" || headerKey.mkString.toLowerCase == "in_network"){
            if (headerKey.mkString.toLowerCase == "provider_references") batch = batch.dropRight(22) //remove this key and write out our batch
            if (headerKey.mkString.toLowerCase == "in_network") batch = batch.dropRight(13) //remove this key and write out our batch
            if (batch(0) != OpenB.toChar) batch.prepend(OpenB.toChar)
            this.synchronized{
              offset = offset + 1
              batches.append((UTF8String.fromString(batch.mkString + "}"), offset.offset))
            }
          }
          else{
            batch.append(i.toChar)
          }
        }
      }
    }

    def parse(i: Int): Unit = {
//      println("Debug Stack Size: " + stack.length)
//      println("Debug batch: " + batch.mkString)
      i match {
        case x if Whitespace.contains(x) =>  if ( isQuoted ) batch.append(i.toChar)
        case Quote => if ( prev != Escape ) isQuoted = !isQuoted
          batch.append(i) //TODO 
        case Colon => if (isHeaderJson) parseHeader(i) else batch.append(i.toChar)
        case OpenB => if ( !isQuoted && prev != Escape ) stack.push(i.toChar)
        case OpenL =>
          if ( !isQuoted && prev != Escape ){ stack.push(i.toChar)
            if ( !isHeaderJson ) batch.append(i.toChar) //if this is a header value we won't write it out (e.g. break apart a list as a seperate json object). The parseHeader()->Colon writes out batch prior to OpenL being seen
          }
        case CloseL =>
          if ( !isQuoted && prev != Escape ){
            if ( stack.top == OpenL ) stack.pop
            else throw new Exception("Sequence mismatch -> Found: " + CloseL.toChar.toString + " Expected Matching: " + stack.top.toChar.toString )
            if ( isHeaderJson ) parseHeader(i)
          }
          batch.append(i.toChar)

        case CloseB =>
          if ( !isQuoted && prev != Escape ) {
            if ( stack.top == OpenB ) stack.pop
            else throw new Exception("Sequence mismatch -> Found: " + CloseB.toChar.toString + " Expected Matching: " + stack.top.toChar.toString )
          }
          batch.append(i.toChar)
        case Comma =>
          if(stack.length != 2) batch.append(i.toChar) //header or deeply nested
          else { //a place where we want to create a split 
            this.synchronized{
              offset = offset + 1
              batches.append((UTF8String.fromString("{" + headerKey.mkString + ":" + batch.mkString), offset.offset))
            }
            batch = ListBuffer.empty[Char]
          }
        case _ => if(isHeader && isQuoted && isKey)
          batch.append(i.toChar)
      }
      prev = i
    }

    def isHeaderJson(): Boolean = (stack.length <= 1)

    /*
     * Defining rules for parsing header only information

    def parseHeader(i: Int): Unit = {
      i match {
        case Quote =>
          if ( prev != Escape ) isQuoted = !isQuoted
          if ( isKey ) headerKey.append(i.toChar)
          batch.append(i.toChar)
        case Colon =>
          if ( !isQuoted ) isKey = !isKey
          batch.append(i.toChar)
        case CloseL =>
          if ( !isQuoted && prev != Escape ) {
            if ( stack.top == OpenL ) stack.pop
            else throw new Exception("Sequence mismatch -> Found: " + CloseL.toChar.toString + " Expected Matching: " + stack.top.toChar.toString )
          }
          batch.append(i.toChar)
      }
    }

     def p(i: Int): Unit = {
      //Deeply Nested List
      else { 
        i match {
          case x if Whitespace.contains(x) =>  if ( isQuoted ) batch.append(i.toChar)
          case Quote =>
            if ( prev != Escape ) isQuoted = !isQuoted
            batch.append(i.toChar)
          case Comma =>
            if ( isQuoted ) batch.append(i.toChar)
            else {
              //Breakup based upon list structure. Assuming schema adherence stack size tells us how deep we are in this structure
              if (stack.length == 2 && stack.top == CloseL){
                this.synchronized{
                  offset = offset + 1
                  batches.append((UTF8String.fromString("{" + batch.mkString + "}"),offset.offset))
                }
                batch = ListBuffer.empty[Char]
              }
            }
          case Colon => batch.append(i.toChar)
          case OpenL => if ( !isQuoted && prev != Escape ) stack.push(OpenL)
            batch.append(i.toChar)
          case OpenB => if ( !isQuoted && prev != Escape ) stack.push(OpenB)
            batch.append(i.toChar)
          case CloseL =>
            if ( !isQuoted && prev != Escape ){
              if ( stack.top == OpenL ) stack.pop
              else throw new Exception("Sequence mismatch -> Found: " + CloseL.toChar.toString + " Expected Matching: " + stack.top.toChar.toString )
            }
            batch.append(i.toChar)
          case CloseB =>
            if ( !isQuoted && prev != Escape ) {
              if ( stack.top == OpenB ) stack.pop
              else throw new Exception("Sequence mismatch -> Found: " + CloseB.toChar.toString + " Expected Matching: " + stack.top.toChar.toString )
            }
            batch.append(i.toChar)
        }
      }
      prev = i
     }
          */

    //Constants
    val Whitespace = Seq(32, 9, 13, 10) //space, tab, CR, LF
    val Escape = 92 // '\'
    val OpenB = 123 // '{'
    val CloseB = 125 // '}'
    val OpenL =  91 // '['
    val CloseL = 93 // ']'
    val Colon = 58 // ':'
    val Comma = 44 // ','
    val Quote = 34 // '"'
  }

  def doSomething(i: Int): Unit = {
    //e.g. below just creates each byte as a seperate value in the stream reader
    val c = i.toChar.toString
    this.synchronized{
      offset = offset + 1
      batches.append((UTF8String.fromString(c), offset.offset))
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

    val data = batches
      .par
      .filter { case (_, idx) => idx >= s && idx <= e}
      .map{  case(v, _) => InternalRow(v) }
      .seq

    val plan = new LocalRelation(Seq(AttributeReference("data", StringType)()), data, isStreaming=true)
    val qe = sqlContext.sparkSession.sessionState.executePlan(plan)
    qe.assertAnalyzed()

    new Dataset(sqlContext.sparkSession, plan, RowEncoder(qe.analyzed.schema)).toDF
  }

  override def stop(): Unit = ???
  /*{
    reader.stop
    fs.close
  }*/


  override def commit(end: Offset): Unit = this.synchronized {
    val committed = (end match {
      case lo: LongOffset => lo
      case _ => LongOffset(-1)
    }).offset
    val toKeep = batches.filter { case (_, idx) => idx > committed }

    println(s"after clean size ${toKeep.length}")
    println(s"deleted: ${batches.size - toKeep.size}")

    batches = toKeep
  }
}

object JsonMRFSource {
  lazy val schema = StructType(List(StructField("json_payload", StringType))) //we're defining a generic string type for our JSON payloads
}
