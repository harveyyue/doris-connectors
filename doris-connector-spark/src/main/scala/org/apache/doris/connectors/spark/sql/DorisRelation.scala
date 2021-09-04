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
import org.apache.doris.connectors.base.rest.RestService
import org.apache.doris.connectors.base.write.{DorisStreamLoadFactory, DorisStreamLoadFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.JavaConverters._

private[sql] class DorisRelation(@transient val sqlContext: SQLContext, parameters: Map[String, String])
  extends BaseRelation with TableScan with PrunedScan with PrunedFilteredScan with InsertableRelation {

  private lazy val inValueLengthLimit = 100

  // Parse spark read/write options to DorisConfiguration
  private lazy val dorisConfig = DorisConfiguration.buildFromMap(parameters.asJava)

  private lazy val lazySchema = SparkSchemaUtils.discoverSchema(dorisConfig)

  private lazy val dialect = JdbcDialects.get("")

  override def schema: StructType = lazySchema

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter(Utils.compileFilter(_, dialect, inValueLengthLimit).isEmpty)
  }

  // TableScan
  override def buildScan(): RDD[Row] = buildScan(Array.empty)

  // PrunedScan
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = buildScan(requiredColumns, Array.empty)

  // PrunedFilteredScan
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // filter where clause can be handled by Doris BE
    val filterWhereClause: String = {
      filters.flatMap(Utils.compileFilter(_, dialect, inValueLengthLimit))
        .map(filter => s"($filter)").mkString(" and ")
    }

    // required columns for column pruner
    val readFields = if (requiredColumns != null && requiredColumns.length > 0) {
      requiredColumns.map(Utils.quote).mkString(",")
    } else {
      lazySchema.fields.map(f => f.name).mkString(",")
    }

    if (filters != null && filters.length > 0) {
      dorisConfig.setFilterQuery(filterWhereClause)
    }
    dorisConfig.setReadFields(readFields)

    new DorisRowRDD(sqlContext.sparkContext, dorisConfig)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (data != null) {
      val ds = DorisStreamLoadFactory.getDorisStreamLoadFormat(dorisConfig.getSinkFileFormatType) match {
        case DorisStreamLoadFormat.JSON => data.toJSON
        case _ => Utils.toCSV(data, dorisConfig.getSinkCsvColumnSeparator)
      }

      if (overwrite) {
        RestService.executeSql(dorisConfig, s"truncate table ${dorisConfig.getTableName}")
      }

      val sparkCtx = data.sqlContext.sparkContext
      sparkCtx.runJob(ds.rdd, new DorisDataFrameWriter(dorisConfig).write _)
    }
  }
}
