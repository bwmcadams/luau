package com.novus.luau.hadoop

/*
 * src/main/scala/hadoop/ConfigHelper.scala
 * created: 04/22/10
 * 
 * Copyright (c) 2009, 2010 Novus Partners, Inc. <http://novus.com>
 * 
 * $Id: scalacommenter.vim 307 2010-04-09 01:07:43Z  $
 * 
 */

import org.apache.hadoop.conf.Configuration

import com.novus.casbah.mongodb.Imports._
/** 
 * 
 * 
 * @author Brendan W. McAdams <bmcadams@novus.com>
 * @version 1.0, 04/22/10
 * @since 1.0
 */
object ConfigHelper {
  val DEFAULT_SPLIT_SIZE = 64 * 1024 // 64k rows

  def splitSize()(implicit config: Configuration): Int = 
    config.getInt("com.novus.luau.mongodb.split_size", DEFAULT_SPLIT_SIZE)

  def mongoConn()(implicit config: Configuration): MongoConnection = {
    // @todo support for clusters/shards/multiple connections, etc. (Driver has left/right et al)
    mongoConn(mongoAddress)
  }

  def mongoConn(addr: DBAddress)(implicit config: Configuration) = MongoConnection(addr)

  def mongoAddress()(implicit config: Configuration): DBAddress = {
    mongoHost match {
      case Some(hostname) => mongoPort match {
        case Some(port) => MongoDBAddress(hostname, port, mongoDB)
        case None => MongoDBAddress(hostname, mongoDB)
      }
      case None => MongoDBAddress("localhost", mongoDB) 
    }
  }


  def mongoHandle()(implicit config: Configuration): MongoCollection =  {
    val conn = mongoConn
    val db  = mongoDB
    val collection = mongoCollection
    conn(db)(collection)
  }

  def mongoHost()(implicit config: Configuration): Option[String] = {
    config.get("com.novus.luau.mongodb.hostname") match {
      case null => None
      case hostname => Some(hostname)
    }
  }
  
  def mongoPort()(implicit config: Configuration): Option[Int] = {
    config.getInt("com.novus.luau.mongodb.port", -1) match {
      case -1 => None
      case port => Some(port)
    }
  }


  /** 
   * mongoCollection
   * 
   * The collection the user requested we send to Hadoop.
   * Note that at the time of this writing (first version)
   * We do NOT support any query to further reduce the content 
   * of this collection.  It is a straight as is call.
   * this IS expected to change as I and the code get more sophisticated.
   * 
   * @since 1.0
   */
  def mongoCollection()(implicit config: Configuration): String = {
    val collection = config.get("com.novus.luau.mongodb.collection")
    require(collection != null)
    collection
  }

  def mongoDB()(implicit config: Configuration): String = {
    val db = config.get("com.novus.luau.mongodb.db")
    require(db != null)
    db
  }
}


// vim: set ts=2 sw=2 sts=2 et:
