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

import org.apache.doris.connectors.base.cfg.DorisConfiguration
import org.apache.doris.connectors.base.exception.DorisException
import org.apache.doris.connectors.base.serialization.Routing
import org.apache.doris.thrift._
import org.apache.thrift.TException
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TSocket, TTransportException}
import org.slf4j.LoggerFactory

class BackendClient(routing: Routing, dorisConfig: DorisConfiguration) {
  val LOG = LoggerFactory.getLogger(getClass)

  var client: TDorisExternalService.Client = null
  var isConnected = false
  val transport = new TSocket(routing.getHost, routing.getPort, dorisConfig.getRequestTimeoutMs)

  def open(): Unit = {
    try {
      transport.open()
      client = new TDorisExternalService.Client(new TBinaryProtocol(transport))
      isConnected = true
    } catch {
      case ex: TTransportException => throw new DorisException(ex.toString)
    }
  }

  def close(): Unit = {
    isConnected = false
    if (transport != null && transport.isOpen) transport.close()
    if (client != null) client = null
  }

  /**
   * Open a scanner for reading Doris data.
   *
   * @param openParams thrift struct to required by request
   * @return scan open result
   * @throws ConnectedFailedException throw if cannot connect to Doris BE
   */
  def openScanner(openParams: TScanOpenParams): TScanOpenResult = {
    LOG.debug(s"OpenScanner to '$routing', parameter is '$openParams'.")
    if (!isConnected) {
      open()
    }
    val result = client.openScanner(openParams)
    if (TStatusCode.OK != result.getStatus.getStatusCode) {
      val error =
        s"""The status of open scanner result from $routing is '${result.getStatus.getStatusCode}',
           |error message is: ${result.getStatus.getErrorMsgs}.""".stripMargin
      throw new DorisException(error)
    }
    result
  }

  /**
   * get next row batch from Doris BE
   *
   * @param nextBatchParams thrift struct to required by request
   * @return scan batch result
   * @throws ConnectedFailedException throw if cannot connect to Doris BE
   */
  def getNext(nextBatchParams: TScanNextBatchParams): TScanBatchResult = {
    LOG.debug(s"GetNext to '$routing', parameter is '$nextBatchParams'.")
    val result = client.getNext(nextBatchParams)
    if (TStatusCode.OK != result.getStatus.getStatusCode) {
      val error =
        s"""The status of get next result from $routing is '${result.getStatus.getStatusCode}',
           |error message is: ${result.getStatus.getErrorMsgs}.""".stripMargin
      throw new DorisException(error)
    }
    result
  }

  /**
   * close an scanner.
   *
   * @param closeParams thrift struct to required by request
   */
  def closeScanner(closeParams: TScanCloseParams): Unit = {
    LOG.debug(s"CloseScanner to '$routing', parameter is '$closeParams'.")
    try {
      client.closeScanner(closeParams)
    } catch {
      case te: TException => LOG.error(te.toString)
    }
  }
}
