package com.novus.luau.hadoop

/*
 * src/main/scala/hadoop/MongoRecordReader.scala
 * created: 04/26/10
 * 
 * Copyright (c) 2009, 2010 Novus Partners, Inc. <http://novus.com>
 * 
 * $Id: scalacommenter.vim 307 2010-04-09 01:07:43Z  $
 * 
 */

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.conf.Configuration

import java.io.{DataInput, DataOutput}

import com.mongodb.{ObjectId => MongoId, DBObject => MongoDBObj, DBAddress => MongoDBAddress}
import com.novus.mongodb.Implicits._
import com.novus.mongodb.ScalaMongoCursorWrapper 


// Tools for working sanely with Java collections
import scala.collection.JavaConversions._
import java.util.{List => JList}

import com.novus.util.Logging

/** 
 * 
 * 
 * @author Brendan W. McAdams <bmcadams@novus.com>
 * @version 1.0, 04/26/10
 * @since 1.0
 */
object MongoRecordReader extends Logging { 
  def apply() = new MongoRecordReader()
}


/** 
 * Hadoop interfaces and structure requires mutability in places 
 * that any sane programmer would use immutability...  Stupid hadoop. 
 * Probably Java's fault though.  Stupid Java.
 * IMMUTABLE GOOD. MUTABLE BAD. ARGH.  REAGAN SMASH!
 * 
 * TODO - We should be supporting authentication...
 *
 * @author Brendan W. McAdams <bmcadams@novus.com>
 * @version 1.0, 04/26/10
 * @since 1.0
 * 
 * @tparam MongoId 
 * @tparam MongoDBObj 
 */
class MongoRecordReader extends RecordReader[MongoId, MongoDBObj] with Logging {
  /* 
   * Since we're required to be mutable, at least be intelligent about it.
   * Because nothings says 'awesome' like a NullPointerException after shredding 3 TB of data
   */
  private var split: Option[MongoSplit] = None
  private var currentRow: Option[MongoDBObj] = None
  private var totalRowCount: Int = -1
  private var cursor: Option[ScalaMongoCursorWrapper[MongoDBObj]] = None

  def initialize(_split: InputSplit, context: TaskAttemptContext) = {
    import ConfigHelper._
    split = Some(_split.asInstanceOf[MongoSplit])
    implicit val config = context.getConfiguration
    totalRowCount = splitSize
    
    log.info("Initializing MongoRecordReader (split = %s, totalRowCount = %d)", split, totalRowCount)
    
    // Setup the cursor range

    log.debug("Attempting to fetch our data from MongoDB")

    val _q = "_id" $in split.get.mongoIDs

    log.trace("Query will be: %s", _q)

    cursor = Some(mongoHandle.find(_q))
     
  }

  /** Hadoop seems to WANT null if things aren't in the right order...*/
  def getCurrentKey: MongoId = currentRow match {
    case Some(row) => row.get("_id").asInstanceOf[MongoId]
    case None => null
  }
  def getCurrentValue: MongoDBObj = currentRow match {
    case Some(row) => row.asInstanceOf[MongoDBObj]
    case None => null
  }
  // Throw a DivByZero exception - not sure how else to handle this cleanly right now
  def getProgress: Float = cursor.get.numSeen / totalRowCount floatValue
  def nextKeyValue(): Boolean = cursor match {
    case Some(_cursor) => {
      if (_cursor.hasNext) {
        currentRow = Some(_cursor.next)
        true
      } else {
        false
      }
    }
    case None => false
  }

  def close() = {}
}
// vim: set ts=2 sw=2 sts=2 et:
