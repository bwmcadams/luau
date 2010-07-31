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
package hadoop

import com.novus.casbah.mongodb.Imports._

import com.novus.casbah.util.Logging

// Tools for working sanely with Java collections
import scala.collection.JavaConversions._
import scalaj.collection.Imports._

import java.util.{List => JList}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce._

class MongoDBOutputFormat extends OutputFormat[ObjectId, DBObject] with Logging {
  import ConfigHelper._

  private val committer = new  MongoDBOutputCommitter 

  def getRecordWriter(context: TaskAttemptContext) = {
    implicit val config = context.getConfiguration
    new MongoDBRecordWriter(mongoOutputHandle, context)
  }

  def checkOutputSpecs(context: JobContext) = {
    implicit val config = context.getConfiguration
    val mongo = mongoOutputHandle
   
    if (mongo.size > 0) 
      throw new Error("Output collection '%s' is not empty.".format(mongoOutputCollection))
  }

  def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    committer
  }

}

// Simple, mostly No-OP OutputCommitter
class MongoDBOutputCommitter extends OutputCommitter with Logging {
  def abortTask(context: TaskAttemptContext) {} 

  def cleanupJob(context: JobContext) {}

  def commitTask(context: TaskAttemptContext) {}

  def needsTaskCommit(taskContext: TaskAttemptContext) = false 

  def setupJob(context: JobContext) {}

  def setupTask(context: TaskAttemptContext) {}
}
// vim: set ts=2 sw=2 sts=2 et:
