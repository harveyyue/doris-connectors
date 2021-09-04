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

package org.apache.doris.connectors.flink.internal.executor;

import org.apache.doris.connectors.base.write.DorisStreamLoad;
import org.apache.doris.connectors.base.write.DorisStreamLoad.DorisBackendSupplier;
import org.apache.doris.connectors.base.write.DorisStreamLoad.DorisFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Currently, this statement executor is only used for table/sql to buffer insert/update/delete
 * events, and reduce them in buffer before submit to doris.
 */
public class TableBufferReducedStreamLoadExecutor implements DorisStreamLoadExecutor<RowData> {

    private final int sinkMaxRetries;
    private final Function<RowData, RowData> keyExtractor;
    private final DorisFunction<RowData, String> valueTransform;
    private DorisStreamLoad dorisStreamLoad;
    // the mapping is [KEY, <+/-, VALUE>]
    private final Map<RowData, Tuple2<Boolean, String>> reduceBuffer = new HashMap<>();

    public TableBufferReducedStreamLoadExecutor(
            int sinkMaxRetries,
            Function<RowData, RowData> keyExtractor,
            DorisFunction<RowData, String> valueTransform) {
        this.sinkMaxRetries = sinkMaxRetries;
        this.keyExtractor = keyExtractor;
        this.valueTransform = valueTransform;
    }

    @Override
    public void prepareDorisStreamLoad(DorisStreamLoad dorisStreamLoad) {
        this.dorisStreamLoad = dorisStreamLoad;
    }

    @Override
    public void addToBatch(RowData record) throws IOException {
        RowData key = keyExtractor.apply(record);
        boolean flag = changeFlag(record.getRowKind());
        String value = valueTransform.apply(record);
        reduceBuffer.put(key, new Tuple2(flag, value));
    }

    @Override
    public void executeBatch(DorisBackendSupplier<String> supplier) throws IOException {
        // upsert
        if (reduceBuffer.values().stream().filter(f -> f.f0).count() > 0) {
            String data = dorisStreamLoad.concatData(
                    reduceBuffer.values().stream().filter(f -> f.f0).map(m -> m.f1).collect(Collectors.toList()));
            dorisStreamLoad.execAndRetry(sinkMaxRetries, data, false, supplier);
        }
        // delete
        if (reduceBuffer.values().stream().filter(f -> !f.f0).count() > 0) {
            String data = dorisStreamLoad.concatData(
                    reduceBuffer.values().stream().filter(f -> !f.f0).map(m -> m.f1).collect(Collectors.toList()));
            dorisStreamLoad.execAndRetry(sinkMaxRetries, data, true, supplier);
        }
        reduceBuffer.clear();
    }

    /**
     * Returns true if the row kind is INSERT or UPDATE_AFTER, returns false if the row kind is
     * DELETE or UPDATE_BEFORE.
     */
    private boolean changeFlag(RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                return true;
            case DELETE:
            case UPDATE_BEFORE:
                return false;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER,"
                                        + " DELETE, but get: %s.",
                                rowKind));
        }
    }
}
