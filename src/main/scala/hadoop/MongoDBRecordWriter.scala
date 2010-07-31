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

package com.novus.luau.hadoop

import com.novus.casbah.mongodb.Imports._

import com.novus.casbah.util.Logging

// Tools for working sanely with Java collections
import scala.collection.JavaConversions._
import scalaj.collection.Imports._

import java.util.{List => JList}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce._

class MongoDBRecordWriter (val mongo: MongoCollection, val context: TaskAttemptContext) extends RecordWriter[ObjectId, DBObject] with Logging {
  private val CONF_OUTPUT_SCHEMA = "com.novus.luau.mongodb.schema"

  log.info("Created a new MongoDBRecordWriter on Output Handle: %s for TaskAttempt: %s", mongo, context.getTaskAttemptID) 

  def write(key: ObjectId, value: DBObject) {
    // todo upsert support (Currently, prepopulated table fails) 
    value.put("_id", key)
    log.debug("Writing Record %s to MongoDB.", value)
    mongo.insert(value)
  }


  def close(context: TaskAttemptContext) {}

}

// vim: set ts=2 sw=2 sts=2 et:
