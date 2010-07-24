package com.novus.luau.hadoop

/*
 * src/main/scala/hadoop/MongoSplit.scala
 * created: 04/26/10
 * 
 * TODO: Look at Scala optimized serializables? 
 *
 * Copyright (c) 2009, 2010 Novus Partners, Inc. <http://novus.com>
 * 
 * $Id: scalacommenter.vim 307 2010-04-09 01:07:43Z  $
 * 
 */

import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.InputSplit

import java.io.{DataInput, DataOutput}



// Tools for working sanely with Java collections
import scala.collection.JavaConversions._
import java.util.{List => JList}

import com.novus.casbah.mongodb.Imports._
import com.novus.casbah.util.Logging

/** 
 * 
 * 
 * @author Brendan W. McAdams <bmcadams@novus.com>
 * @version 1.0, 04/26/10
 * @since 1.0
 */
object MongoSplit extends Logging {
  def apply(mongoIDs: Set[ObjectId], mongoServers: Array[DBAddress]) = 
    new MongoSplit(mongoIDs, mongoServers)

  def read(in: DataInput) = {
    val numIds = in.readInt
    val ids = for (x <- 0 until numIds) yield new ObjectId(in.readUTF)

    
    val numServers = in.readInt

    val servers = for (x <- 0 until numServers) yield MongoDBAddress(in.readUTF)

    log.debug("Statically deserialized MongoSplit {ids = %s, length = %d, servers = %s",
              ids, ids.size, servers)
    apply(ids toSet, servers.toArray)
  }

  def write(split: MongoSplit, out: DataOutput) = {
    out.writeInt(split.mongoIDs.size)
    for (id <- split.mongoIDs) {
      out.writeUTF(id toString)
    }
    out.writeInt(split.mongoServers.size)
    for (srv <- split.mongoServers) {
      out.writeUTF(srv toString)
    }
  }
}

/** 
 * 
 * We're supposed to simply, with this, represent the range of value keys.
 * No actual VALUES are represented here, the RecordReader reinstantiates
 * which means there's a risk that the values in here may not exist by
 * the time the RecordReader gets to it.  Not sure yet how to handle/ expect that
 *
 * @author Brendan W. McAdams <bmcadams@novus.com>
 * @version 1.0, 04/26/10
 * @since 1.0
 * 
 */
@scala.reflect.BeanInfo
class MongoSplit(@scala.reflect.BeanProperty val mongoIDs: Set[ObjectId], 
                 val mongoServers: Array[DBAddress]) 
        extends InputSplit with Logging {
  assume(mongoIDs.size > 0)
  log.debug("Instantiated MongoSplit %s", this)

  def getLocations = mongoServers.map(_.toString)

  def getLength = mongoIDs.size

  override def toString: String = 
    "{MongoSplit [ids (%s) of size %d on serverList: %s]".
      format(mongoIDs, mongoIDs.size, getLocations)
  
}

// vim: set ts=2 sw=2 sts=2 et:
