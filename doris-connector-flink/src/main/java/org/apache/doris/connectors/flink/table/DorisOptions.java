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

package org.apache.doris.connectors.flink.table;

import org.apache.doris.connectors.base.cfg.DorisConfiguration;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT;
import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT;
import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_EXEC_MEM_LIMIT_DEFAULT;
import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_REQUEST_BATCH_SIZE_DEFAULT;
import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT;
import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_REQUEST_RETRIES_DEFAULT;
import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_REQUEST_TIMEOUT_MS_DEFAULT;
import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_SINK_BUFFER_FLUSH_MAX_ROWS_DEFAULT;
import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_SINK_CSV_COLUMN_SEPARATOR_DEFAULT;
import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_SINK_CSV_LINE_DELIMITER_DEFAULT;
import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_SINK_FILE_FORMAT_TYPE_DEFAULT;
import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_SINK_MAX_RETRIES_DEFAULT;
import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_SINK_SUPPORT_FILE_FORMAT_TYPES;
import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_TABLET_SIZE_DEFAULT;

public class DorisOptions {
    // Doris required options
    public static final ConfigOption<String> FENODES = ConfigOptions
            .key("doris.fenodes")
            .stringType()
            .noDefaultValue()
            .withDescription("doris fe http address.");

    public static final ConfigOption<String> USERNAME = ConfigOptions
            .key("doris.username")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc user name.");

    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("doris.password")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc password.");

    public static final ConfigOption<String> TABLE_IDENTIFIER = ConfigOptions
            .key("doris.table.identifier")
            .stringType()
            .noDefaultValue()
            .withDescription("the table identifier name.");

    // Doris read options
    private static final ConfigOption<String> DORIS_READ_FIELD = ConfigOptions
            .key("doris.read.field")
            .stringType()
            .defaultValue("*")
            .withDescription("List of column names in the Doris table, separated by commas");

    private static final ConfigOption<String> DORIS_FILTER_QUERY = ConfigOptions
            .key("doris.filter.query")
            .stringType()
            .noDefaultValue()
            .withDescription("Filter expression of the query, which is transparently transmitted to Doris. " +
                    "Doris uses this expression to complete source-side data filtering");

    private static final ConfigOption<Integer> DORIS_TABLET_SIZE = ConfigOptions
            .key("doris.request.tablet.size")
            .intType()
            .defaultValue(DORIS_TABLET_SIZE_DEFAULT)
            .withDescription("");

    private static final ConfigOption<Integer> DORIS_REQUEST_TIMEOUT_MS = ConfigOptions
            .key("doris.request.timeout.ms")
            .intType()
            .defaultValue(DORIS_REQUEST_TIMEOUT_MS_DEFAULT)
            .withDescription("");

    private static final ConfigOption<Integer> DORIS_REQUEST_QUERY_TIMEOUT_S = ConfigOptions
            .key("doris.request.query.timeout.s")
            .intType()
            .defaultValue(DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT)
            .withDescription("");

    private static final ConfigOption<Integer> DORIS_REQUEST_RETRIES = ConfigOptions
            .key("doris.request.retries")
            .intType()
            .defaultValue(DORIS_REQUEST_RETRIES_DEFAULT)
            .withDescription("");

    private static final ConfigOption<Boolean> DORIS_DESERIALIZE_ARROW_ASYNC = ConfigOptions
            .key("doris.deserialize.arrow.async")
            .booleanType()
            .defaultValue(DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT)
            .withDescription("");

    private static final ConfigOption<Integer> DORIS_DESERIALIZE_QUEUE_SIZE = ConfigOptions
            .key("doris.deserialize.queue.size")
            .intType()
            .defaultValue(DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT)
            .withDescription("");

    private static final ConfigOption<Integer> DORIS_REQUEST_BATCH_SIZE = ConfigOptions
            .key("doris.request.batch.size")
            .intType()
            .defaultValue(DORIS_REQUEST_BATCH_SIZE_DEFAULT)
            .withDescription("");

    private static final ConfigOption<Long> DORIS_EXEC_MEM_LIMIT = ConfigOptions
            .key("doris.exec.mem.limit")
            .longType()
            .defaultValue(DORIS_EXEC_MEM_LIMIT_DEFAULT)
            .withDescription("");

    // Doris write options
    private static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS = ConfigOptions
            .key("doris.sink.buffer-flush.max-rows")
            .intType()
            .defaultValue(DORIS_SINK_BUFFER_FLUSH_MAX_ROWS_DEFAULT)
            .withDescription("the flush max size (includes all append, upsert and delete records), over this number" +
                    " of records, will flush data. The default value is 100.");

    private static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("doris.sink.buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("The flush interval mills, over this time, asynchronous threads will flush data.");

    private static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions
            .key("doris.sink.max-retries")
            .intType()
            .defaultValue(DORIS_SINK_MAX_RETRIES_DEFAULT)
            .withDescription("the max retry times if writing records to database failed.");

    private static final ConfigOption<String> SINK_FILE_FORMAT_TYPE = ConfigOptions
            .key("doris.sink.file.format.type")
            .stringType()
            .defaultValue(DORIS_SINK_FILE_FORMAT_TYPE_DEFAULT)
            .withDescription("stream load file format type, support csv, json");

    private static final ConfigOption<String> SINK_CSV_COLUMN_SEPARATOR = ConfigOptions
            .key("doris.sink.csv.column.separator")
            .stringType()
            .defaultValue(DORIS_SINK_CSV_COLUMN_SEPARATOR_DEFAULT)
            .withDescription("column separator for csv format.");

    private static final ConfigOption<String> SINK_CSV_LINE_DELIMITER = ConfigOptions
            .key("doris.sink.csv.line.delimiter")
            .stringType()
            .defaultValue(DORIS_SINK_CSV_LINE_DELIMITER_DEFAULT)
            .withDescription("line delimiter for csv format.");

    public static Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet();
        options.add(FENODES);
        options.add(TABLE_IDENTIFIER);
        options.add(USERNAME);
        options.add(PASSWORD);
        return options;
    }

    public static Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet();
        // read options
        options.add(DORIS_READ_FIELD);
        options.add(DORIS_FILTER_QUERY);
        options.add(DORIS_TABLET_SIZE);
        options.add(DORIS_REQUEST_TIMEOUT_MS);
        options.add(DORIS_REQUEST_QUERY_TIMEOUT_S);
        options.add(DORIS_REQUEST_RETRIES);
        options.add(DORIS_REQUEST_BATCH_SIZE);
        options.add(DORIS_EXEC_MEM_LIMIT);
        options.add(DORIS_DESERIALIZE_ARROW_ASYNC);
        options.add(DORIS_DESERIALIZE_QUEUE_SIZE);
        // write options
        options.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        options.add(SINK_BUFFER_FLUSH_INTERVAL);
        options.add(SINK_MAX_RETRIES);
        options.add(SINK_FILE_FORMAT_TYPE);
        options.add(SINK_CSV_COLUMN_SEPARATOR);
        options.add(SINK_CSV_LINE_DELIMITER);
        return options;
    }


    public static DorisConfiguration getSourceDorisConfiguration(ReadableConfig config) {
        return DorisConfiguration
                .builder()
                .withFenodes(config.get(FENODES))
                .withTableIdentifier(config.get(TABLE_IDENTIFIER))
                .withUsername(config.get(USERNAME))
                .withPassword(config.get(PASSWORD))
                .withDeserializeArrowAsync(config.get(DORIS_DESERIALIZE_ARROW_ASYNC))
                .withDeserializeQueueSize(config.get(DORIS_DESERIALIZE_QUEUE_SIZE))
                .withExecMemLimit(config.get(DORIS_EXEC_MEM_LIMIT))
                .withFilterQuery(config.get(DORIS_FILTER_QUERY))
                .withRequestQueryTimeoutS(config.get(DORIS_REQUEST_QUERY_TIMEOUT_S))
                .withRequestBatchSize(config.get(DORIS_REQUEST_BATCH_SIZE))
                .withRequestTimeoutMs(config.get(DORIS_REQUEST_TIMEOUT_MS))
                .withRequestRetries(config.get(DORIS_REQUEST_RETRIES))
                .withRequestTabletSize(config.get(DORIS_TABLET_SIZE))
                .build();
    }

    public static DorisConfiguration getSinkDorisConfiguration(ReadableConfig config) {
        return DorisConfiguration
                .builder()
                .withFenodes(config.get(FENODES))
                .withTableIdentifier(config.get(TABLE_IDENTIFIER))
                .withUsername(config.get(USERNAME))
                .withPassword(config.get(PASSWORD))
                .withSinkBatchSize(config.get(SINK_BUFFER_FLUSH_MAX_ROWS))
                .withSinkBatchIntervalMs(config.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis())
                .withSinkMaxRetries(config.get(SINK_MAX_RETRIES))
                .withSinkFileFormatType(config.get(SINK_FILE_FORMAT_TYPE))
                .withSinkCsvColumnSeparator(config.get(SINK_CSV_COLUMN_SEPARATOR))
                .withSinkCsvLineDelimiter(config.get(SINK_CSV_LINE_DELIMITER))
                .build();
    }

    public static void validateSinkFileFormatType(ReadableConfig config) {
        String formatType = config.get(SINK_FILE_FORMAT_TYPE);
        if (!DORIS_SINK_SUPPORT_FILE_FORMAT_TYPES.contains(formatType.toUpperCase())) {
            throw new ValidationException(
                    String.format("Unsupported value '%s' for %s. Supported values are [CSV, JSON].",
                            formatType, SINK_FILE_FORMAT_TYPE.key()));
        }
    }
}
