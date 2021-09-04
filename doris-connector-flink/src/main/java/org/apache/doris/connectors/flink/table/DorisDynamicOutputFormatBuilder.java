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
import org.apache.doris.connectors.base.write.DorisStreamLoad.DorisFunction;
import org.apache.doris.connectors.base.write.DorisStreamLoadFactory;
import org.apache.doris.connectors.flink.internal.executor.DorisStreamLoadExecutor;
import org.apache.doris.connectors.flink.internal.executor.TableBufferReducedStreamLoadExecutor;
import org.apache.doris.connectors.flink.internal.executor.TableBufferedStreamLoadExecutor;
import org.apache.doris.connectors.flink.internal.options.DorisDmlOptions;
import org.apache.flink.formats.json.RowDataToJsonConverters.RowDataToJsonConverter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.function.Function;

import static org.apache.flink.table.data.RowData.createFieldGetter;

/**
 * Builder for {@link DorisOutputFormat} for Table/SQL.
 */
public class DorisDynamicOutputFormatBuilder implements Serializable {

    private static final long serialVersionUID = 1L;

    private DorisConfiguration dorisConfig;
    private DorisDmlOptions dorisDmlOptions;
    private RowDataToJsonConverter converter;
    private DataType[] fieldDataTypes;

    private ObjectMapper objectMapper =
            new ObjectMapper().configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);

    public DorisDynamicOutputFormatBuilder(
            DorisConfiguration dorisConfig,
            DorisDmlOptions dorisDmlOptions,
            RowDataToJsonConverter converter,
            DataType[] fieldDataTypes) {
        this.dorisConfig = dorisConfig;
        this.dorisDmlOptions = dorisDmlOptions;
        this.converter = converter;
        this.fieldDataTypes = fieldDataTypes;
    }

    public DorisOutputFormat<RowData, ?> build() {
        LogicalType[] logicalTypes =
                Arrays.stream(fieldDataTypes)
                        .map(DataType::getLogicalType)
                        .toArray(LogicalType[]::new);

        if (dorisDmlOptions.getKeyFields().isPresent() && dorisDmlOptions.getKeyFields().get().length > 0) {
            return new DorisOutputFormat(dorisConfig, () ->
                    createBufferReduceExecutor(dorisConfig, dorisDmlOptions, logicalTypes));
        } else {
            return new DorisOutputFormat(dorisConfig, () -> createBufferedExecutor(dorisConfig));
        }
    }

    private DorisStreamLoadExecutor<RowData> createBufferReduceExecutor(
            DorisConfiguration dorisConfig,
            DorisDmlOptions dorisDmlOptions,
            LogicalType[] logicalTypes) {
        int[] pkFields =
                Arrays.stream(dorisDmlOptions.getKeyFields().get())
                        .mapToInt(Arrays.asList(dorisDmlOptions.getFieldNames())::indexOf)
                        .toArray();
        DorisFunction<RowData, String> valueTransform = row -> getDorisValue(row);
        return new TableBufferReducedStreamLoadExecutor(
                dorisConfig.getSinkMaxRetries(),
                createRowKeyExtractor(logicalTypes, pkFields),
                valueTransform);
    }

    private DorisStreamLoadExecutor<RowData> createBufferedExecutor(DorisConfiguration dorisConfig) {
        DorisFunction<RowData, String> valueTransform = row -> getDorisValue(row);
        return new TableBufferedStreamLoadExecutor(dorisConfig.getSinkMaxRetries(), valueTransform);
    }

    private String getDorisValue(RowData row) throws IOException {
        String value;
        switch (DorisStreamLoadFactory.getDorisStreamLoadFormat(dorisConfig.getSinkFileFormatType())) {
            case JSON:
                ObjectNode node = objectMapper.createObjectNode();
                converter.convert(objectMapper, node, row);
                value = objectMapper.writeValueAsString(node);
                break;
            default:
                StringJoiner joiner = new StringJoiner(dorisConfig.getSinkCsvColumnSeparator());
                GenericRowData rowData = (GenericRowData) row;
                for (int i = 0; i < row.getArity(); ++i) {
                    joiner.add(rowData.getField(i) == null ? null : rowData.getField(i).toString());
                }
                value = joiner.toString();
                break;
        }
        return value;
    }

    private Function<RowData, RowData> createRowKeyExtractor(LogicalType[] logicalTypes, int[] pkFields) {
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[pkFields.length];
        for (int i = 0; i < pkFields.length; i++) {
            fieldGetters[i] = createFieldGetter(logicalTypes[pkFields[i]], pkFields[i]);
        }
        return row -> getPrimaryKey(row, fieldGetters);
    }

    private RowData getPrimaryKey(RowData row, RowData.FieldGetter[] fieldGetters) {
        GenericRowData pkRow = new GenericRowData(fieldGetters.length);
        for (int i = 0; i < fieldGetters.length; i++) {
            pkRow.setField(i, fieldGetters[i].getFieldOrNull(row));
        }
        return pkRow;
    }
}
