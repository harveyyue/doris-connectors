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

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.sources._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

import java.util.StringJoiner

private[sql] object Utils {
  /**
   * quote column name
   *
   * @param colName column name
   * @return quoted column name
   */
  def quote(colName: String): String = s"`$colName`"

  /**
   * compile a filter to Doris FE filter format.
   *
   * @param filter             filter to be compile
   * @param dialect            jdbc dialect to translate value to sql format
   * @param inValueLengthLimit max length of in value array
   * @return if Doris FE can handle this filter, return None if Doris FE can not handled it.
   */
  def compileFilter(filter: Filter, dialect: JdbcDialect, inValueLengthLimit: Int): Option[String] = {
    Option(filter match {
      case EqualTo(attribute, value) => s"${quote(attribute)} = ${dialect.compileValue(value)}"
      case GreaterThan(attribute, value) => s"${quote(attribute)} > ${dialect.compileValue(value)}"
      case GreaterThanOrEqual(attribute, value) => s"${quote(attribute)} >= ${dialect.compileValue(value)}"
      case LessThan(attribute, value) => s"${quote(attribute)} < ${dialect.compileValue(value)}"
      case LessThanOrEqual(attribute, value) => s"${quote(attribute)} <= ${dialect.compileValue(value)}"
      case In(attribute, values) =>
        if (values.isEmpty || values.length >= inValueLengthLimit) {
          null
        } else {
          s"${quote(attribute)} in (${dialect.compileValue(values)})"
        }
      case IsNull(attribute) => s"${quote(attribute)} is null"
      case IsNotNull(attribute) => s"${quote(attribute)} is not null"
      case And(left, right) =>
        val and = Seq(left, right).flatMap(compileFilter(_, dialect, inValueLengthLimit))
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" and ")
        } else {
          null
        }
      case Or(left, right) =>
        val or = Seq(left, right).flatMap(compileFilter(_, dialect, inValueLengthLimit))
        if (or.size == 2) {
          or.map(p => s"($p)").mkString(" or ")
        } else {
          null
        }
      case _ => null
    })
  }

  def toCSV(data: DataFrame, columnSeparator: String): Dataset[String] = {
    data.mapPartitions(iter =>
      new Iterator[String] {
        override def hasNext: Boolean = iter.hasNext

        override def next(): String = {
          val value = new StringJoiner(columnSeparator)
          val row = iter.next()
          for (i <- 0 until row.length) {
            value.add(if (row.get(i) == null) null else row.get(i).toString)
          }
          value.toString
        }
      }
    )(Encoders.STRING)
  }
}
