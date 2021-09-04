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

package org.apache.doris.connectors.base.cfg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public interface ConfigurationOptions {
    String DORIS_DEFAULT_CLUSTER = "default_cluster";
    // doris fe node address
    String DORIS_FENODES = "doris.fenodes";
    String DORIS_USER = "doris.user";
    String DORIS_PASSWORD = "doris.password";
    String DORIS_TABLE_IDENTIFIER = "doris.table.identifier";

    String DORIS_READ_FIELD = "doris.read.field";
    String DORIS_READ_FIELD_DEFAULT = "*";
    String DORIS_FILTER_QUERY = "doris.filter.query";
    String DORIS_FILTER_QUERY_IN_MAX_COUNT = "doris.filter.query.in.max.count";
    int DORIS_FILTER_QUERY_IN_VALUE_UPPER_LIMIT = 10000;

    String DORIS_REQUEST_TIMEOUT_MS = "doris.request.timeout.ms";
    int DORIS_REQUEST_TIMEOUT_MS_DEFAULT = 30 * 1000;

    String DORIS_REQUEST_RETRIES = "doris.request.retries";
    int DORIS_REQUEST_RETRIES_DEFAULT = 3;

    String DORIS_REQUEST_BATCH_SIZE = "doris.request.batch.size";
    int DORIS_REQUEST_BATCH_SIZE_DEFAULT = 1024;

    String DORIS_REQUEST_QUERY_TIMEOUT_S = "doris.request.query.timeout.s";
    int DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT = 3600;

    String DORIS_TABLET_SIZE = "doris.request.tablet.size";
    int DORIS_TABLET_SIZE_DEFAULT = Integer.MAX_VALUE;
    int DORIS_TABLET_SIZE_MIN = 1;

    String DORIS_EXEC_MEM_LIMIT = "doris.exec.mem.limit";
    long DORIS_EXEC_MEM_LIMIT_DEFAULT = 2147483648L;

    String DORIS_DESERIALIZE_QUEUE_SIZE = "doris.deserialize.queue.size";
    int DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT = 64;

    String DORIS_DESERIALIZE_ARROW_ASYNC = "doris.deserialize.arrow.async";
    boolean DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT = false;

    String DORIS_SINK_BUFFER_FLUSH_MAX_ROWS = "doris.sink.buffer-flush.max-rows";
    int DORIS_SINK_BUFFER_FLUSH_MAX_ROWS_DEFAULT = 100;

    String DORIS_SINK_BUFFER_FLUSH_INTERVAL = "doris.sink.buffer-flush.interval";
    long DORIS_SINK_BUFFER_FLUSH_INTERVAL_DEFAULT = 1000;

    String DORIS_SINK_MAX_RETRIES = "doris.sink.max-retries";
    int DORIS_SINK_MAX_RETRIES_DEFAULT = 3;

    String DORIS_SINK_FILE_FORMAT_TYPE = "doris.sink.file.format.type";
    String DORIS_SINK_FILE_FORMAT_TYPE_DEFAULT = "CSV";

    String DORIS_SINK_CSV_COLUMN_SEPARATOR = "doris.sink.csv.column.separator";
    String DORIS_SINK_CSV_COLUMN_SEPARATOR_DEFAULT = "\t";

    String DORIS_SINK_CSV_LINE_DELIMITER = "doris.sink.csv.line.delimiter";
    String DORIS_SINK_CSV_LINE_DELIMITER_DEFAULT = "\n";

    String DORIS_BLANK = "";
    List<String> DORIS_SINK_SUPPORT_FILE_FORMAT_TYPES = new ArrayList<>(Arrays.asList("CSV", "JSON"));
    List<String> DORIS_STREAM_LOAD_SUCCESS_STATUS = new ArrayList<>(Arrays.asList("Success", "Publish Timeout"));
}
