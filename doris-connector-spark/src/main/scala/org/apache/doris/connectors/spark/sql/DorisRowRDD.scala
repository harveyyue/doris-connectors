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
import org.apache.doris.connectors.base.rest.models.PartitionDefinition
import org.apache.doris.connectors.spark.rdd.{AbstractDorisRDD, AbstractDorisRDDIterator, DorisPartition}
import org.apache.doris.connectors.spark.serialization.SparkRowBatch
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

private[spark] class DorisRowRDD(sc: SparkContext, dorisConfig: DorisConfiguration)
  extends AbstractDorisRDD[Row](sc, dorisConfig) {

  override def compute(split: Partition, context: TaskContext): DorisRowRDDIterator = {
    new DorisRowRDDIterator(context, split.asInstanceOf[DorisPartition].dorisPartition, dorisConfig)
  }
}

private[spark] class DorisRowRDDIterator(context: TaskContext,
                                         partition: PartitionDefinition,
                                         dorisConfig: DorisConfiguration)
  extends AbstractDorisRDDIterator[Row](context) {

  override val reader = {
    initialized = true
    new SparkRowValueReader(partition, dorisConfig)((result, schema) => new SparkRowBatch(result, schema))
  }

  override def createValue(value: Object): Row = {
    value.asInstanceOf[DorisRow]
  }
}
