// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.connectors.spark.sql

import org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_DEFAULT_CLUSTER
import org.apache.doris.connectors.base.cfg.DorisConfiguration
import org.apache.doris.connectors.base.util.CacheManager
import org.apache.doris.connectors.base.write.DorisStreamLoad.DorisBackendSupplier
import org.apache.doris.connectors.base.write.DorisStreamLoadFactory
import org.apache.spark.TaskContext

import java.io.IOException
import java.util.Collections
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

private[spark] class DorisDataFrameWriter(dorisConfig: DorisConfiguration) extends Serializable {

  def write(taskContext: TaskContext, data: Iterator[String]): Unit = {
    taskContext.partitionId()
    val backendCache = CacheManager.buildBackendCache(dorisConfig, 5, TimeUnit.MINUTES)

    def getRandomBackend(): String = {
      val backends = backendCache.get(DORIS_DEFAULT_CLUSTER)
      if (backends.size == 0) {
        throw new IOException("No available be")
      }
      Collections.shuffle(backends)
      backends.get(0)
    }

    def supplier(): DorisBackendSupplier[String] = new DorisBackendSupplier[String] {
      override def getBackend(): String = getRandomBackend
    }

    val dorisStreamLoad = DorisStreamLoadFactory.instance(dorisConfig, getRandomBackend)
    val batch = new ArrayBuffer[String]()
    var count = 0

    data.foreach(row => {
      batch.append(row)
      count += 1

      if (dorisConfig.getSinkBatchSize > 0 && count >= dorisConfig.getSinkBatchSize) {
        val status = dorisStreamLoad.execAndRetry(
          dorisConfig.getSinkMaxRetries, dorisStreamLoad.concatData(batch.asJava), supplier)
        if (status) {
          count = 0
          batch.clear()
        }
      }
    })

    if (batch.size > 0) {
      dorisStreamLoad.execAndRetry(
        dorisConfig.getSinkMaxRetries, dorisStreamLoad.concatData(batch.asJava), supplier)
    }
  }
}
