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

package org.apache.doris.connectors.base.read

import org.apache.doris.connectors.base.cfg.ConfigurationOptions._
import org.apache.doris.connectors.base.cfg.DorisConfiguration
import org.apache.doris.connectors.base.exception.ShouldNeverHappenException
import org.apache.doris.connectors.base.rest.SchemaUtils
import org.apache.doris.connectors.base.rest.models.{PartitionDefinition, Schema}
import org.apache.doris.connectors.base.serialization.{Routing, RowBatch}
import org.apache.doris.connectors.base.util.ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE
import org.apache.doris.thrift._
import org.apache.log4j.Logger

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConversions._
import scala.util.control.Breaks

class ScalaValueReader(partition: PartitionDefinition, dorisConfig: DorisConfiguration)
                      (f: (TScanBatchResult, Schema) => RowBatch) {

  protected val LOG = Logger.getLogger(classOf[ScalaValueReader])

  protected val client = new BackendClient(new Routing(partition.getBeAddress), dorisConfig)
  protected var offset = 0
  protected var eos: AtomicBoolean = new AtomicBoolean(false)
  protected var rowBatch: RowBatch = _
  // flag indicate if support deserialize Arrow to RowBatch asynchronously
  protected var deserializeArrowToRowBatchAsync = dorisConfig.isDeserializeArrowAsync

  protected var rowBatchBlockingQueue: BlockingQueue[RowBatch] = {
    if (deserializeArrowToRowBatchAsync) {
      new ArrayBlockingQueue(dorisConfig.getDeserializeQueueSize)
    } else {
      null
    }
  }

  private val openParams: TScanOpenParams = {
    val params = new TScanOpenParams
    params.cluster = DORIS_DEFAULT_CLUSTER
    params.database = partition.getDatabase
    params.table = partition.getTable
    params.tablet_ids = partition.getTabletIds.toList
    params.opaqued_query_plan = partition.getQueryPlan

    params.batch_size = dorisConfig.getRequestBatchSize
    params.query_timeout = dorisConfig.getRequestQueryTimeoutS
    params.mem_limit = dorisConfig.getExecMemLimit
    params.user = dorisConfig.getUsername
    params.passwd = dorisConfig.getPassword

    LOG.debug(s"Open scan params is, ${params.toString}")

    params
  }

  protected val openResult: TScanOpenResult = client.openScanner(openParams)
  protected val contextId: String = openResult.getContextId
  protected val schema: Schema = SchemaUtils.convertToSchema(openResult.getSelectedColumns)
  LOG.debug(s"Open scan result is, contextId: $contextId, schema: $schema.")

  protected val asyncThread: Thread = new Thread {
    override def run {
      val nextBatchParams = new TScanNextBatchParams
      nextBatchParams.setContextId(contextId)
      while (!eos.get) {
        nextBatchParams.setOffset(offset)
        val nextResult = client.getNext(nextBatchParams)
        eos.set(nextResult.isEos)
        if (!eos.get) {
          val rowBatch = f(nextResult, schema).readArrow()
          offset += rowBatch.getReadRowCount
          rowBatch.close
          rowBatchBlockingQueue.put(rowBatch)
        }
      }
    }
  }

  protected val asyncThreadStarted: Boolean = {
    var started = false
    if (deserializeArrowToRowBatchAsync) {
      asyncThread.start
      started = true
    }
    started
  }

  /**
   * read data and cached in rowBatch.
   *
   * @return true if hax next value
   */
  def hasNext: Boolean = {
    var hasNext = false
    if (deserializeArrowToRowBatchAsync && asyncThreadStarted) {
      // support deserialize Arrow to RowBatch asynchronously
      if (rowBatch == null || !rowBatch.hasNext) {
        val loop = new Breaks
        loop.breakable {
          while (!eos.get || !rowBatchBlockingQueue.isEmpty) {
            if (!rowBatchBlockingQueue.isEmpty) {
              rowBatch = rowBatchBlockingQueue.take
              hasNext = true
              loop.break
            } else {
              // wait for rowBatch put in queue or eos change
              Thread.sleep(5)
            }
          }
        }
      } else {
        hasNext = true
      }
    } else {
      // Arrow data was acquired synchronously during the iterative process
      if (!eos.get && (rowBatch == null || !rowBatch.hasNext)) {
        if (rowBatch != null) {
          offset += rowBatch.getReadRowCount
          rowBatch.close
        }
        val nextBatchParams = new TScanNextBatchParams
        nextBatchParams.setContextId(contextId)
        nextBatchParams.setOffset(offset)
        val nextResult = client.getNext(nextBatchParams)
        eos.set(nextResult.isEos)
        if (!eos.get) {
          rowBatch = f(nextResult, schema).readArrow()
        }
      }
      hasNext = !eos.get
    }
    hasNext
  }

  /**
   * get next value.
   *
   * @return next value
   */
  def next: AnyRef = {
    if (!hasNext) {
      LOG.error(SHOULD_NOT_HAPPEN_MESSAGE)
      throw new ShouldNeverHappenException
    }
    rowBatch.next
  }

  def close(): Unit = {
    val closeParams = new TScanCloseParams
    closeParams.context_id = contextId
    client.closeScanner(closeParams)
  }
}
