/**
 * Copyright (c) 2009, 2010 Novus Partners, Inc. <http://novus.com>
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For questions and comments about this product, please see the project page at:
 *
 *     http://github.com/novus/luau
 * 
 */

package com.novus.luau
package pig


import com.novus.luau.hadoop._

import com.novus.casbah.mongodb.Imports._

import com.novus.casbah.util.Logging

// Tools for working sanely with Java collections
import scala.collection.JavaConversions._
import scalaj.collection.Imports._

import scala.collection.immutable.List

import java.util.{List => JList}

import org.apache.hadoop.mapreduce._
import org.apache.pig._
import org.apache.pig.data._
import org.apache.pig.impl.util.UDFContext


class MongoDBStorer extends StoreFunc with StoreMetadata with Logging {
  import ConfigHelper._
  
  private var udfContextSignature: String = null
  private var writer: MongoDBRecordWriter = null
 
  private val CONF_LUAU_URI = "com.novus.luau.mongodb.uri"
  private val CONF_LUAU_OUTPUT_COLLECTION = "com.novus.luau.mongodb.collection.output"
  private val CONF_OUTPUT_SCHEMA = "com.novus.luau.mongodb.schema"
  private val UDFCONTEXT_OUTPUT_SCHEMA = "UDF.com.novus.luau.mongodb.schema"

  override def checkSchema(_schema: ResourceSchema) { 
    val properties = UDFContext.getUDFContext.getUDFProperties(this.getClass, Array[String](udfContextSignature));
    properties.setProperty(UDFCONTEXT_OUTPUT_SCHEMA, MongoDBPigSchema(_schema))
  }

  def storeSchema(_schema: ResourceSchema, location: String, job: Job) {
    log.info("store Schema %s in %s for job %s", _schema, location, job)
  }

  def storeStatistics(_stats: ResourceStatistics, location: String, job: Job) {
    log.info("store Stats %s in %s for job %s", _stats, location, job)
  }


  def putNext(tuple: Tuple) {
    import ConfigHelper._
    // TODO - fill me in
    implicit val config = writer.context.getConfiguration
    val sStr = config.get(CONF_OUTPUT_SCHEMA)
    log.debug("Schema String: %s", sStr)
    val schema = sStr.split(",").toList
    log.debug("Got a tuple to store: %s [Schema: %s]", tuple, schema)
    log.debug("Stored Schema: %s", schema)
    val builder = MongoDBObject.newBuilder
    for (i <- Range(0, tuple.size)) {
      builder += schema(i) -> tuple.get(i)
    }
    val dbObj = builder.result.asDBObject
    log.trace("Serializing: %s", dbObj)
    writer.write(null, dbObj)
  }

  def getOutputFormat() = new MongoDBOutputFormat

  def prepareToWrite(_writer: RecordWriter[_, _]) = _writer match {
    case mongoWriter: MongoDBRecordWriter => writer = mongoWriter
    case invalid => throw new java.io.IOException("Invalid writer type. Expected a MongoDBWriter, but got a '%s'".format(invalid.getClass))
  }

  override def relToAbsPathForStoreLocation(location: String, curDir: org.apache.hadoop.fs.Path) = {
    log.debug("relToAbsPathForStoreLocation - location: %s, curDir: %s", location, curDir)
    location 
  }

  def setStoreLocation(location: String, job: Job) {
    implicit val config = job.getConfiguration
    log.info("Setting store location with a URI: %s", location)
    val addr = 
      if (location.startsWith("mongodb://")) 
        location.split("mongodb://")(1)
      else
        throw new IllegalArgumentException("URIs must begin with a protocol indicator of mongodb://")

    val uri = addr.split("#")
    log.info("URI Constituents: %s", uri)
    require(uri.size == 2, "URI Format must contain a collection after a '#', e.g. localhost/myDB#myCollection")
    config.set(CONF_LUAU_URI, uri(0))
    config.set(CONF_LUAU_OUTPUT_COLLECTION, uri(1))
    val properties = UDFContext.getUDFContext.getUDFProperties(this.getClass, Array[String](udfContextSignature));
    config.set(CONF_OUTPUT_SCHEMA, properties.getProperty(UDFCONTEXT_OUTPUT_SCHEMA));
  }

  override def setStoreFuncUDFContextSignature(signature: String) = {
    log.debug("StoreFuncUDFContextSignature: %s", signature)
    udfContextSignature = signature
  }
  
}

object MongoDBPigSchema  extends Logging {
  def apply(_schema: ResourceSchema): String = {
    val builder = List.newBuilder[String]
    log.debug("[write schema] Got a Resource Schema: %s", _schema.fieldNames.toSet)
    for (field <- _schema.fieldNames) {
      log.info(" -> Field: %s", field)
      builder += field
    }
    val schema = builder.result
    log.info("Schema Data: %s", schema)
    val out = schema.mkString(",")
    log.info("Output writing: %s", out)
    out
  }

  //def apply(schema: String) = schema.split(",").toSet
     
}


// vim: set ts=2 sw=2 sts=2 et:
