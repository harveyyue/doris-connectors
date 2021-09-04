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

import org.apache.doris.connectors.base.cfg.DorisConfiguration
import org.apache.doris.connectors.base.exception.ShouldNeverHappenException
import org.apache.doris.connectors.base.read.ScalaValueReader
import org.apache.doris.connectors.base.rest.models.{PartitionDefinition, Schema}
import org.apache.doris.connectors.base.serialization.RowBatch
import org.apache.doris.connectors.base.util.ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE
import org.apache.doris.thrift.TScanBatchResult
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

class SparkRowValueReader(partition: PartitionDefinition, dorisConfig: DorisConfiguration)
                         (f: (TScanBatchResult, Schema) => RowBatch)
  extends ScalaValueReader(partition, dorisConfig)(f) with Logging {

  val rowOrder: Seq[String] = dorisConfig.getReadFields.split(",")

  override def next: AnyRef = {
    if (!hasNext) {
      logError(SHOULD_NOT_HAPPEN_MESSAGE)
      throw new ShouldNeverHappenException
    }

    val row = new DorisRow(rowOrder)
    rowBatch.next().asScala.zipWithIndex.foreach {
      case (s, index) if index < row.values.size => row.values.update(index, s)
      case _ => // nothing
    }
    row
  }
}
