package com.databricks.labs.sparkstreaming.jsonmrf

import collection.mutable.Stack
object ByteParser{

    /*
     *  @param arr: array to parse assuming schema compliance
     *  @param: arrLength the length of the array populated
     *  Return: first key found moving right to left, along with the beginning offset value right after the Quote
     *  
     */
  def searchKeyRight(arr: Array[Byte], arrLength: Int, arrStart: Int): (Option[String], Int) = {
    if ( arrLength <= 1 ) return (None, -1)
    for (i <- arrStart to 1 by -1){
      arr(i).toInt match {
        case Colon =>
          var s = skipWhiteSpaceRight(arr, i-1, arrLength) //find first nonewhitespace char after i
          if ( s <= 1 ) return (None, -1)
          if ( arr(s).toInt == Quote ) {
            getQuotedStringRight(arr, s, arrLength) match {
              case Some(x) => return (Some(x), s-x.length)
              case None =>
            }
          }
        case _ =>
      }
    }
    return (None, -1)
  }

    /*
     * Returns the quoted String approaching from the right
     *  Start Index must be double quote 
     */
  def getQuotedStringRight(arr: Array[Byte], startIndex: Int, arrLength: Int, maxStringLength: Int=25): Option[String] = {
    if ( arr(startIndex).toInt != Quote ) return None
    for(i <- startIndex-1 to 1.max(startIndex-maxStringLength) by -1){
      arr(i).toInt match {
        case Quote => if ( arr(i-1).toInt != Escape ){
          return Some(arr.slice(i+1, startIndex).map(_.toChar).mkString )
        }
        case _ =>
      }
    }
    return None
  }

    /*
     *  Assuming Reading from beginning of JSON object
     *  Return the index of the first '['
     */
  def parseUntilArrayLeft(arr: Array[Byte], arrLength: Int, arrStart: Int=0): Int = {
    var isQuoted = false
    var prevChar = -1
    if ( arrLength == 0 ) throw new Exception("Tried to find an opening brace '[' on an empty array")
    for (i<- arrStart to arrLength -1){
      arr(i).toInt match {
        case Quote => isQuoted = !isQuoted
        case OpenL => if ( prevChar!=Escape && !isQuoted) return i
        case _ =>
      }
      prevChar=arr(i)
    }
    return EOB
  }

  // @param startIndex expected the end of an element of an array, either '}' or ']'.
  // @return found: index of the beginning of next array element, e.g. first value after ',' (this could be whitespace)
  //        not-found: -1 
  def arrayHasNext(arr: Array[Byte], startIndex: Int, arrLength: Int): Int = {
    if ( arr(startIndex).toInt != CloseB && arr(startIndex).toInt != CloseL ) throw new Exception("Did not see a correct startIndex of a json element to determine if the array ended. Please make sure this value is either } or ]")
    var i = findByteArrayEndingLeft(arr, startIndex+1, arrLength)
    var j = findByteLeft(arr, Comma, startIndex+1, i)
    if ( j >= 0 && j < i ) return skipWhiteSpaceLeft(arr, j+1, arrLength)
    else if ( i == EOB ) return EOB
    else return -1
  }

    /*
     *  @param arr: array to parse assuming schema compliance
     *  @param arrLength: the length of the array populated
     *  @param startIndex: the first location to start looking
     *  @param b: the byte (as integer) to find (does not account for quoted text)
     * 
     *  @Return: return the index into the array 
     */
  def findByteRight(arr: Array[Byte], b: Int, startIndex: Int, arrLength: Int): Int = {
    if ( arrLength <= 1 ) return -1
    for (i <- startIndex to 1 by -1){ //we cannot check the validity of an unescaped first character
      if (arr(i).toInt == b && arr(i-1).toInt != Escape)  return i
      }
    return -1
  }


  def findByteLeft(arr: Array[Byte], b: Int, startIndex: Int, arrLength: Int): Int = {
    if ( arrLength <= 1 || startIndex == 0 ) return -1
    var i  = startIndex 
    for (i <- startIndex to arrLength -1 ){ 
      if (arr(i).toInt == b && arr(i-1).toInt != Escape)  return i
    }
    if ( i == arrLength -1 ) return EOB
    return -1 
  }

    /*
     * Skips any whitespace characters approach from the right and return the index
     */
  def skipWhiteSpaceRight(arr: Array[Byte], startIndex: Int, arrLength: Int): Int = {
    if ( arrLength <= 1 || startIndex == 0 ) return -1
    for (i <- startIndex to 0 by -1){
      arr(i).toInt match {
        case x if Whitespace.contains(x) =>
        case _ => return i
      }
    }
    return -1
  }

  def skipWhiteSpaceLeft(arr: Array[Byte], startIndex: Int, arrLength: Int): Int = {
    if ( arrLength <= 1 || startIndex == arrLength ) return EOB
    for (i <- startIndex to arrLength -1 ){
      arr(i).toInt match {
        case x if Whitespace.contains(x) =>
        case _ => return i
      }
    }
    return EOB
  }

  /*
   * Find the beginning of a new json element from the start index 
   */
  def findByteArrayBeginningLeft(arr: Array[Byte], startIndex: Int, arrLength: Int): Int = {
    for ( i <- startIndex to arrLength -1){
      arr(i).toInt match {
        case OpenB | OpenL => return i
        case _ => 
      }
    }
    return EOB
  }

  /*
   * Finding a closing brace or bracket ']' at the startIndex
   */
  def findByteArrayEndingLeft(arr: Array[Byte], startIndex: Int, arrLength: Int): Int = {
    for ( i <- startIndex to arrLength -1){
      arr(i).toInt match {
        case CloseB | CloseL => return i
        case _ => 
      }
    }
    return EOB
  }



  /*
   * startIndex is expected to be after the outer [, and either start on or before first element in json Array '{' or '['
   *  @return (idx of last valid array element end, idx of end of array) 
   */
  def seekEndOfArray(arr: Array[Byte], startIndexTmp: Int, arrLength: Int): (Int, Int) = {
    val startIndex = skipWhiteSpaceLeft(arr, startIndexTmp, arrLength)
    startIndex match {
      case EOB => return (EOB, EOB)
      case x =>
        if(arr(x).toInt != OpenB && arr(x).toInt != OpenL )
          throw new Exception("Error: Did not see a correct startIndex of a json element to determine if the array ended. Please make sure this value is either { or [. First byte consumed:" + arr(startIndexTmp).toChar + " secondary bytes..." + arr.slice(startIndexTmp, startIndex+20).map(_.toChar).mkString )
    }
    var validCutoff = -1 
    var i: Int = startIndex
    do{
      seekMatchingEndBracket(arr, i, arrLength) match {
        case EOB => return (validCutoff, EOB) //Buffer cuts a record in half
        case x => //x becomes our new validcutoff point
          validCutoff = x
          i = arrayHasNext(arr, x, arrLength)
          i match {
            case EOB => return (validCutoff, EOB) //reached EOB stream
            case -1 => return (validCutoff, findByteLeft(arr, CloseL, validCutoff, arrLength)) //array does not have next 
            case _ => //array has next 
          }
      }
    } while ( i > 0 ) 
    return (validCutoff, EOB)
  }

  /*
   * Return the first position that does not contain a comma or whitespace starting at startindex
   *  move left to right
   */
  def skipWhiteSpaceAndCommaLeft(arr: Array[Byte], startIndex: Int, arrLength: Int): Int = {
    for ( i <- startIndex to arrLength -1 ) {
      arr(i).toInt match {
        case x if Whitespace.contains(x) =>
        case Comma =>
        case _ => return i 
      }
    }
    return EOB
  }

    /*
   * Return the first position that does not contain a comma or whitespace starting at startindex
   *  move left to right
   */
  def skipWhiteSpaceAndCommaRight(arr: Array[Byte], startIndex: Int, arrLength: Int): Int = {
    for ( i <- startIndex to 0 by -1) {
      arr(i).toInt match {
        case x if Whitespace.contains(x) =>
        case Comma =>
        case _ => return i 
      }
    }
    return EOB
  }

  /*
   * Assume just seen an openingB or openingL, find the index of the closingB/closeL that matches
   *   this could be called at the end of a list where the only remaining value is a } or ]
   *  
   */
  def seekMatchingEndBracket(arr: Array[Byte], startIndexTmp: Int, arrLength: Int): Int = {
    val startIndex = skipWhiteSpaceLeft(arr, startIndexTmp, arrLength)
    startIndex match {
      case EOB => return EOB
      case x =>
        if ( arr(startIndex).toInt != OpenB && arr(startIndex).toInt != OpenL )
          throw new Exception("Did not see a correct opening brace/bracket to start function. startIndex " +startIndex +
            " Last Byte consumed: " + arr(startIndex).toChar)
    }

    var prev = -1
    var stack = Stack.empty[Int]
    var isQuoted = false
    for ( i <- startIndex to arrLength -1 ) {
      arr(i).toInt match {
        case OpenB => if ( !isQuoted && prev != Escape ) stack.push(OpenB)
        case CloseB => if ( !isQuoted && prev != Escape) {
          if( stack.top != OpenB ) throw new Exception("Sequence mismatch! found } and expecting matching " + stack.top.toChar)
          if ( stack.size == 1 )  return i
          else stack.pop
        }
        case OpenL => if ( !isQuoted && prev != Escape ) stack.push(OpenL)
        case CloseL => if ( !isQuoted && prev != Escape ) {
          if ( stack.top != OpenL ) throw new Exception("Sequence mismatch! found ] and expecting matching " + stack.top.toChar)
          if ( stack.size ==1 )	return i
          else stack.pop
        }
        case Quote => if ( prev != Escape ) isQuoted = !isQuoted
        case _ => 
      }
      prev = arr(i).toInt
    }
    return EOB
  }


  val Whitespace = Seq(32, 9, 13, 10) //space, tab, CR, LF
  val Escape = 92
  val OpenB = 123 // '{'
  val CloseB = 125 // '}'
  val OpenL =  91 // '['
  val CloseL = 93 // ']'
  val Colon = 58 // ':'
  val Comma = 44 // ','
  val Quote = 34 // '"'
  val EOB = -2 // represent an end of buffer
}
