package com.novus.luau.hadoop
/*
 * src/main/scala/hadoop/MongoDBInputFormat.scala
 * created: 04/22/10
 * 
 * Copyright (c) 2009, 2010 Novus Partners, Inc. <http://novus.com>
 * 
 * $Id: scalacommenter.vim 307 2010-04-09 01:07:43Z  $
 * 
 */

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.conf.Configuration

import com.mongodb.{ObjectId => MongoId, DBObject => MongoDBObj} // just make it clear it's a mongo id...


// Tools for working sanely with Java collections
import scala.collection.JavaConversions._
import scalaj.collection.Imports._
import java.util.{List => JList}

import scala.actors.Futures._
import scala.collection._

import com.novus.util.Logging

/** 
 * InputFormat for Hadoop which reads directly from MongoDB
 * according to a particular specification.
 * Currently very basic and woefully unoptimised.
 *
 * Based looseley upon Cassandra's Hadoop interfacing.
 * @see https://svn.apache.org/repos/asf/cassandra/trunk/src/java/org/apache/cassandra/hadoop/
 * @see 
 *
 * @author Brendan W. McAdams <bmcadams@novus.com>
 * @version 1.0, 04/22/10
 * @since 1.0
 */
class MongoDBInputFormat extends InputFormat[MongoId, MongoDBObj] with Logging {
  import ConfigHelper._

  def getSplits(context: JobContext): JList[InputSplit] = {
    implicit val config = context.getConfiguration
    
    checkConfig 

    // setup a mongo connection to work with
    val mongo =  mongoHandle
    log.info("Instantiated a connection (w/ db & collection) to Mongo: %s", mongo)

    /**
     * The idea here, as in the Cassandra code, is to split the collection
     * into pieces according to the defined split size.  It also should be done
     * in parallel - the Cassandra example, in Java uses Executor/Futures based thread pools
     * But we should be able to do it effectively with scala's builtin concurrency tools.
     */
    
      
    /**
     * Mongo works with cursors which means
     * nothing is explicitly shipped downwire
     * with this call - we'll use ranges.
     * Snapshotting also to ensure consistency
     * --- SNAPSHOTTING CURRENTLY DISABLED 
     * TODO - Make snapshotting a toggleable option for those who like to live dangerously 
     * TODO - Snapshotting can't be used with sorted/hinted collection.  Do we require sorting more for efficiency of split m/rs?
     * Should we also set the cursor's batch Size to our split size.
     * This *SHOULD* Send us a single bulk of $batchSize per next call on the cursor, and be efficient.
     * ... Just make sure you synchronize the cursor 
     * TODO - Test if limit + skip is more efficient/performant/safer/shinier
     */
    //val cursor = mongo.find.snapshot
    val cursor = mongo.find
    val cursorSize = cursor.count
    log.info("Retrieved a cursor from mongo of size %d.  Set batchSize of %d", cursorSize, splitSize)
    //  Attempt to spin up a future 
    val splitFutures = for {
      n <- 0 until splitSize / cursorSize + 1 // Number of splits -  should serve as a proper flow control
      // Future for splitting. SHOULD return a Seq[MongoSplit] if all goes well.
      val fSplitter = future {
        // Synchronize on the cursor ... May or may not be very efficient thread wise.  
        cursor.synchronized {
          val startSeen = cursor.numSeen
          val mongoIds =  mutable.HashSet[MongoId]()
          // TODO - Check if we need to iterate one more or if the -1 is ok 
          for (row <- cursor if (cursor.hasNext && (cursor.numSeen < startSeen + splitSize - 1))) {
            mongoIds += row.get("_id").asInstanceOf[MongoId]
          }
          val seenItems = startSeen - cursor.numSeen 
          assume(seenItems == mongoIds.size)
          log.trace("Saw %s items.  First ID: %s, Last ID: %s", seenItems, mongoIds.head, mongoIds.last)
          // Make the id set immutable on the way out  
          // TODO - Just capture it as an immutable via yield in the for and save memory
          MongoSplit(mongoIds.toSet, Array(mongoAddress))
        }
      }
    } yield fSplitter

    log.debug("Constructed the futures: %s", splitFutures)

    val splits = splitFutures map { f => f() } 
    assume(splits.size > 0)
    log.info("Returning %s split objects.")
    splits.asJava
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[MongoId, MongoDBObj] = {
    implicit val config = context.getConfiguration
    MongoRecordReader()
  }

  def checkConfig()(implicit config: Configuration) {
    // @TODO Parse and verify configuration file for any attributes we care about

  }
}

// vim: set ts=2 sw=2 sts=2 et:
