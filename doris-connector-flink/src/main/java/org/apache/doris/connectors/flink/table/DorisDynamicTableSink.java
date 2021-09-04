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
import org.apache.doris.connectors.flink.internal.options.DorisDmlOptions;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.RowDataToJsonConverters;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

/**
 * DorisDynamicTableSink
 **/
public class DorisDynamicTableSink implements DynamicTableSink {

    private final DorisConfiguration dorisConfig;
    private final DorisDmlOptions dorisDmlOptions;
    private final TableSchema tableSchema;

    public DorisDynamicTableSink(
            DorisConfiguration dorisConfig,
            DorisDmlOptions dorisDmlOptions,
            TableSchema tableSchema) {
        this.dorisConfig = dorisConfig;
        this.dorisDmlOptions = dorisDmlOptions;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataType[] fieldDataTypes = tableSchema.getFieldDataTypes();
        RowDataToJsonConverters.RowDataToJsonConverter converter =
                new RowDataToJsonConverters(TimestampFormat.SQL, JsonOptions.MapNullKeyMode.DROP, null)
                        .createConverter(tableSchema.toRowDataType().getLogicalType());

        return OutputFormatProvider.of(
                new DorisDynamicOutputFormatBuilder(dorisConfig, dorisDmlOptions, converter, fieldDataTypes).build());
    }

    @Override
    public DynamicTableSink copy() {
        return new DorisDynamicTableSink(dorisConfig, dorisDmlOptions, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "Doris Table Sink";
    }
}
