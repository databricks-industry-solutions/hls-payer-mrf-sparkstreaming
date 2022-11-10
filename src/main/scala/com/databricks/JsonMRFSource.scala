package com.databricks.labs.sparkstreaming.jsonmrf

//import org.apache.hadoop.io.compress.CompressionInputStream
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
      /*
       Iterator.continually(inStream.read)
        .takeWhile(_ != -1)
       .foreach(doSomething)
       */
    }
  }

  /*
   * For now, assume valid in-network schema
   *  https://github.com/CMSgov/price-transparency-guide/tree/master/schemas/in-network-rates
   */
  object JsonParser {

    var prev = -1
    var isQuoted = false
    var batch = ListBuffer.empty[Char]
    var stack = Stack.empty[Int]
    // Parse header...
    // read until [
    // parse out lists until matching ]
    // discover next special char, { or [ ?
    // if [ repeat "parse out lists"
    // else parse header
    // close
    def parse(i: Int): Unit = {
      i match {
        case x if Whitespace.contains(x) =>
          if ( isQuoted ) batch.append(i.toChar)
        case Quote =>
          if ( prev != Escape ) isQuoted = !isQuoted
          batch.append(i.toChar)
        case Comma =>
          if ( isQuoted ) batch.append(i.toChar)
          else {
            //Breakup based upon list structure. Assuming schema adherence stack size tells us how deep we are in this structure
            if (stack.length == 3 && stack.top == CloseB){
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
        case CloseL => if ( !isQuoted && prev != Escape ) ??? //stack.pop
          batch.append(i.toChar)
        case CloseB => if ( !isQuoted && prev != Escape ) ??? //stack.pop
          batch.append(i.toChar)
      }
      prev = i
    }

    //Constants
    val Whitespace = Seq(32, 9, 13, 10) //space, tab, CR, LF
    val Escape = 92 // '\'
    val OpenB = 134 // '{'
    val CloseB = 173 // '}'
    val OpenL =  133 // '['
    val CloseL = 135 // ']'
    val Colon = 72 // ':'
    val Comma = 54 // ','
    val Quote = 42 // '"'
  }

  def doSomething(i: Int): Unit = {
    //TODO iterate over the file... parsing out appropriate breaks in data

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
