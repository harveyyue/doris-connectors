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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.doris.connectors.base.util.Preconditions;
import org.apache.doris.connectors.base.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_SINK_SUPPORT_FILE_FORMAT_TYPES;

/**
 * Doris configuration
 */
public class DorisConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DorisConfiguration.class);

    // Doris connector
    @JsonProperty(value = ConfigurationOptions.DORIS_FENODES)
    private String fenodes;

    @JsonProperty(value = ConfigurationOptions.DORIS_USER)
    private String username;

    @JsonProperty(value = ConfigurationOptions.DORIS_PASSWORD)
    private String password;

    @JsonProperty(value = ConfigurationOptions.DORIS_TABLE_IDENTIFIER)
    private String tableIdentifier;

    // Doris read options
    @JsonProperty(value = ConfigurationOptions.DORIS_READ_FIELD)
    private String readFields;

    @JsonProperty(value = ConfigurationOptions.DORIS_FILTER_QUERY)
    private String filterQuery;

    @JsonProperty(value = ConfigurationOptions.DORIS_REQUEST_TIMEOUT_MS)
    private int requestTimeoutMs;

    @JsonProperty(value = ConfigurationOptions.DORIS_TABLET_SIZE)
    private int requestTabletSize;

    @JsonProperty(value = ConfigurationOptions.DORIS_REQUEST_QUERY_TIMEOUT_S)
    private int requestQueryTimeoutS;

    @JsonProperty(value = ConfigurationOptions.DORIS_REQUEST_RETRIES)
    private int requestRetries;

    @JsonProperty(value = ConfigurationOptions.DORIS_REQUEST_BATCH_SIZE)
    private int requestBatchSize;

    @JsonProperty(value = ConfigurationOptions.DORIS_EXEC_MEM_LIMIT)
    private long execMemLimit;

    @JsonProperty(value = ConfigurationOptions.DORIS_DESERIALIZE_QUEUE_SIZE)
    private int deserializeQueueSize;

    @JsonProperty(value = ConfigurationOptions.DORIS_DESERIALIZE_ARROW_ASYNC)
    private boolean deserializeArrowAsync;

    // Doris write options
    @JsonProperty(value = ConfigurationOptions.DORIS_SINK_BUFFER_FLUSH_MAX_ROWS)
    private int sinkBatchSize;

    @JsonProperty(value = ConfigurationOptions.DORIS_SINK_BUFFER_FLUSH_INTERVAL)
    private long sinkBatchIntervalMs;

    @JsonProperty(value = ConfigurationOptions.DORIS_SINK_MAX_RETRIES)
    private int sinkMaxRetries;

    @JsonProperty(value = ConfigurationOptions.DORIS_SINK_FILE_FORMAT_TYPE)
    private String sinkFileFormatType;

    @JsonProperty(value = ConfigurationOptions.DORIS_SINK_CSV_COLUMN_SEPARATOR)
    private String sinkCsvColumnSeparator;

    @JsonProperty(value = ConfigurationOptions.DORIS_SINK_CSV_LINE_DELIMITER)
    private String sinkCsvLineDelimiter;

    public DorisConfiguration(
            String fenodes,
            String username,
            String password,
            String tableIdentifier,
            String readFields,
            String filterQuery,
            int requestTimeoutMs,
            int requestTabletSize,
            int requestQueryTimeoutS,
            int requestRetries,
            int requestBatchSize,
            long execMemLimit,
            int deserializeQueueSize,
            boolean deserializeArrowAsync,
            int sinkBatchSize,
            long sinkBatchIntervalMs,
            int sinkMaxRetries,
            String sinkFileFormatType,
            String sinkCsvColumnSeparator,
            String sinkCsvLineDelimiter) {
        this.fenodes = fenodes;
        this.username = username;
        this.password = password;
        this.tableIdentifier = tableIdentifier;
        this.readFields = readFields;
        this.filterQuery = filterQuery;
        this.requestTimeoutMs = requestTimeoutMs;
        this.requestTabletSize = requestTabletSize;
        this.requestQueryTimeoutS = requestQueryTimeoutS;
        this.requestRetries = requestRetries;
        this.requestBatchSize = requestBatchSize;
        this.execMemLimit = execMemLimit;
        this.deserializeQueueSize = deserializeQueueSize;
        this.deserializeArrowAsync = deserializeArrowAsync;
        this.sinkBatchSize = sinkBatchSize;
        this.sinkBatchIntervalMs = sinkBatchIntervalMs;
        this.sinkMaxRetries = sinkMaxRetries;
        this.sinkFileFormatType = sinkFileFormatType;
        this.sinkCsvColumnSeparator = sinkCsvColumnSeparator;
        this.sinkCsvLineDelimiter = sinkCsvLineDelimiter;
    }

    public String getFenodes() {
        return fenodes;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }

    public String getDbName() {
        return tableIdentifier.split("\\.")[0];
    }

    public String getTableName() {
        return tableIdentifier.split("\\.")[1];
    }

    public String getReadFields() {
        return readFields;
    }

    public void setReadFields(String readFields) {
        this.readFields = readFields;
    }

    public String getFilterQuery() {
        return filterQuery;
    }

    public void setFilterQuery(String filterQuery) {
        this.filterQuery = filterQuery;
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public int getRequestTabletSize() {
        return requestTabletSize;
    }

    public int getRequestQueryTimeoutS() {
        return requestQueryTimeoutS;
    }

    public int getRequestRetries() {
        return requestRetries;
    }

    public int getRequestBatchSize() {
        return requestBatchSize;
    }

    public long getExecMemLimit() {
        return execMemLimit;
    }

    public int getDeserializeQueueSize() {
        return deserializeQueueSize;
    }

    public boolean isDeserializeArrowAsync() {
        return deserializeArrowAsync;
    }

    public int getSinkBatchSize() {
        return sinkBatchSize;
    }

    public long getSinkBatchIntervalMs() {
        return sinkBatchIntervalMs;
    }

    public int getSinkMaxRetries() {
        return sinkMaxRetries;
    }

    public String getSinkFileFormatType() {
        return sinkFileFormatType;
    }

    public String getSinkCsvColumnSeparator() {
        return sinkCsvColumnSeparator;
    }

    public String getSinkCsvLineDelimiter() {
        return sinkCsvLineDelimiter;
    }

    public static DorisConfiguration buildFromMap(Map<String, String> properties) throws Exception {
        properties = new HashMap<>(properties);
        DorisConfiguration dorisConfig =
                new DorisConfiguration.Builder()
                        .withFenodes(properties.get(ConfigurationOptions.DORIS_FENODES))
                        .withUsername(properties.get(ConfigurationOptions.DORIS_USER))
                        .withPassword(properties.get(ConfigurationOptions.DORIS_PASSWORD))
                        .withTableIdentifier(properties.get(ConfigurationOptions.DORIS_TABLE_IDENTIFIER))
                        .build();
        properties.remove(ConfigurationOptions.DORIS_FENODES);
        properties.remove(ConfigurationOptions.DORIS_USER);
        properties.remove(ConfigurationOptions.DORIS_PASSWORD);
        properties.remove(ConfigurationOptions.DORIS_TABLE_IDENTIFIER);

        Field[] fields = dorisConfig.getClass().getDeclaredFields();
        List<String> annotationValues = new ArrayList<>();
        for (Field field : fields) {
            field.setAccessible(true);
            JsonProperty jsonProperty = field.getDeclaredAnnotation(JsonProperty.class);
            if (jsonProperty == null) {
                continue;
            }
            String key = jsonProperty.value();
            annotationValues.add(key);
            if (StringUtils.isBlank(properties.get(key))) {
                continue;
            }
            String value = properties.get(key);
            String type = field.getType().getSimpleName();
            switch (type) {
                case "String":
                    field.set(dorisConfig, value);
                    break;
                case "int":
                    field.setInt(dorisConfig, Integer.valueOf(value));
                    break;
                case "long":
                    field.setLong(dorisConfig, Long.valueOf(value));
                    break;
                case "boolean":
                    field.setBoolean(dorisConfig, Boolean.valueOf(value));
                    break;
                default:
                    break;
            }
        }
        // Check unavailable properties
        List<String> invalidProperties = properties.keySet().stream()
                .filter(f -> !annotationValues.contains(f))
                .collect(Collectors.toList());
        if (invalidProperties.size() > 0) {
            LOG.warn("Unavailable doris properties: {}",
                    invalidProperties.stream().collect(Collectors.joining(",")));
        }
        return dorisConfig;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder of {@link DorisConfiguration}.
     */
    public static class Builder {
        // Doris connector
        private String fenodes;
        private String username;
        private String password;
        private String tableIdentifier;
        // Doris read options
        private String readFields = ConfigurationOptions.DORIS_READ_FIELD_DEFAULT;
        private String filterQuery = ConfigurationOptions.DORIS_BLANK;
        private int requestTimeoutMs = ConfigurationOptions.DORIS_REQUEST_TIMEOUT_MS_DEFAULT;
        private int requestTabletSize = ConfigurationOptions.DORIS_TABLET_SIZE_DEFAULT;
        private int requestQueryTimeoutS = ConfigurationOptions.DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT;
        private int requestRetries = ConfigurationOptions.DORIS_REQUEST_RETRIES_DEFAULT;
        private int requestBatchSize = ConfigurationOptions.DORIS_REQUEST_BATCH_SIZE_DEFAULT;
        private long execMemLimit = ConfigurationOptions.DORIS_EXEC_MEM_LIMIT_DEFAULT;
        private int deserializeQueueSize = ConfigurationOptions.DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT;
        private boolean deserializeArrowAsync = ConfigurationOptions.DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT;
        // Doris write options
        private int sinkBatchSize = ConfigurationOptions.DORIS_SINK_BUFFER_FLUSH_MAX_ROWS_DEFAULT;
        private long sinkBatchIntervalMs = ConfigurationOptions.DORIS_SINK_BUFFER_FLUSH_INTERVAL_DEFAULT;
        private int sinkMaxRetries = ConfigurationOptions.DORIS_SINK_MAX_RETRIES_DEFAULT;
        private String sinkFileFormatType = ConfigurationOptions.DORIS_SINK_FILE_FORMAT_TYPE_DEFAULT;
        private String sinkCsvColumnSeparator = ConfigurationOptions.DORIS_SINK_CSV_COLUMN_SEPARATOR_DEFAULT;
        private String sinkCsvLineDelimiter = ConfigurationOptions.DORIS_SINK_CSV_LINE_DELIMITER_DEFAULT;

        public Builder withFenodes(String fenodes) {
            this.fenodes = fenodes;
            return this;
        }

        public Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder withTableIdentifier(String tableIdentifier) {
            this.tableIdentifier = tableIdentifier;
            return this;
        }

        public Builder withReadFields(String readFields) {
            this.readFields = readFields;
            return this;
        }

        public Builder withFilterQuery(String filterQuery) {
            this.filterQuery = filterQuery;
            return this;
        }

        public Builder withRequestTimeoutMs(int requestTimeoutMs) {
            this.requestTimeoutMs = requestTimeoutMs;
            return this;
        }

        public Builder withRequestTabletSize(int requestTabletSize) {
            this.requestTabletSize = requestTabletSize;
            return this;
        }

        public Builder withRequestQueryTimeoutS(int requestQueryTimeoutS) {
            this.requestQueryTimeoutS = requestQueryTimeoutS;
            return this;
        }

        public Builder withRequestRetries(int requestRetries) {
            this.requestRetries = requestRetries;
            return this;
        }

        public Builder withRequestBatchSize(int requestBatchSize) {
            this.requestBatchSize = requestBatchSize;
            return this;
        }

        public Builder withExecMemLimit(long execMemLimit) {
            this.execMemLimit = execMemLimit;
            return this;
        }

        public Builder withDeserializeQueueSize(int deserializeQueueSize) {
            this.deserializeQueueSize = deserializeQueueSize;
            return this;
        }

        public Builder withDeserializeArrowAsync(boolean deserializeArrowAsync) {
            this.deserializeArrowAsync = deserializeArrowAsync;
            return this;
        }

        public Builder withSinkBatchSize(int sinkBatchSize) {
            this.sinkBatchSize = sinkBatchSize;
            return this;
        }

        public Builder withSinkBatchIntervalMs(long sinkBatchIntervalMs) {
            this.sinkBatchIntervalMs = sinkBatchIntervalMs;
            return this;
        }

        public Builder withSinkMaxRetries(int sinkMaxRetries) {
            this.sinkMaxRetries = sinkMaxRetries;
            return this;
        }

        public Builder withSinkFileFormatType(String sinkFileFormatType) {
            this.sinkFileFormatType = sinkFileFormatType;
            return this;
        }

        public Builder withSinkCsvColumnSeparator(String sinkCsvColumnSeparator) {
            this.sinkCsvColumnSeparator = sinkCsvColumnSeparator;
            return this;
        }

        public Builder withSinkCsvLineDelimiter(String sinkCsvLineDelimiter) {
            this.sinkCsvLineDelimiter = sinkCsvLineDelimiter;
            return this;
        }

        public DorisConfiguration build() {
            Preconditions.checkNotNull(fenodes, "No fenodes supplied.");
            Preconditions.checkNotNull(username, "No username supplied.");
            Preconditions.checkNotNull(password, "No password supplied.");
            Preconditions.checkNotNull(tableIdentifier, "No tableIdentifier supplied.");
            Preconditions.checkArgument(tableIdentifier.split("\\.").length == 2,
                    "Error tableIdentifier format.");
            Preconditions.checkNotNull(DORIS_SINK_SUPPORT_FILE_FORMAT_TYPES.contains(sinkFileFormatType.toUpperCase()),
                    String.format("Unsupported value '%s'. Supported values are [CSV, JSON].", sinkFileFormatType));
            return new DorisConfiguration(
                    fenodes, username, password, tableIdentifier, readFields, filterQuery, requestTimeoutMs,
                    requestTabletSize, requestQueryTimeoutS, requestRetries, requestBatchSize, execMemLimit,
                    deserializeQueueSize, deserializeArrowAsync, sinkBatchSize, sinkBatchIntervalMs, sinkMaxRetries,
                    sinkFileFormatType, sinkCsvColumnSeparator, sinkCsvLineDelimiter);
        }
    }
}
