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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * The {@link DorisDynamicTableFactory} translates the catalog table to a table source.
 *
 * <p>Because the table source requires a decoding format, we are discovering the format using the
 * provided {@link FactoryUtil} for convenience.
 */
public final class DorisDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "doris";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER; // used for matching to `connector = '...'`
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return DorisOptions.requiredOptions();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return DorisOptions.optionalOptions();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // validate all options
        helper.validate();
        // get the validated options
        final ReadableConfig options = helper.getOptions();
        // derive the produced data type (excluding computed columns) from the catalog table
        final DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        String readFields = physicalSchema.getTableColumns().stream().map(m -> String.format("`%s`", m.getName()))
                .collect(Collectors.joining(","));
        DorisConfiguration dorisConfig = DorisOptions.getSourceDorisConfiguration(options);
        // keep reading the same doris columns and orders are consistency according the catalog table
        dorisConfig.setReadFields(readFields);
        // create and return dynamic table source
        return new DorisDynamicTableSource(dorisConfig, physicalSchema);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // validate all options
        helper.validate();
        final ReadableConfig options = helper.getOptions();
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        // create and return dynamic table sink
        DorisConfiguration dorisConfig = DorisOptions.getSinkDorisConfiguration(options);
        return new DorisDynamicTableSink(dorisConfig, getDorisDmlOptions(dorisConfig, physicalSchema), physicalSchema);
    }

    private DorisDmlOptions getDorisDmlOptions(DorisConfiguration dorisConfig, TableSchema schema) {
        String[] keyFields =
                schema.getPrimaryKey()
                        .map(pk -> pk.getColumns().toArray(new String[0]))
                        .orElse(null);
        return DorisDmlOptions.builder()
                .withFieldNames(schema.getFieldNames())
                .withKeyFields(keyFields)
                .withTableName(dorisConfig.getTableIdentifier())
                .build();
    }
}
