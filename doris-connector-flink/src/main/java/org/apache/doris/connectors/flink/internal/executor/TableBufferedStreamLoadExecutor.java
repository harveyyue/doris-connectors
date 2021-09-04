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
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Currently, this statement executor is only used for table/sql to buffer records.
 */
public class TableBufferedStreamLoadExecutor implements DorisStreamLoadExecutor<RowData> {

    private final int sinkMaxRetries;
    private final DorisFunction<RowData, String> valueTransform;
    private DorisStreamLoad dorisStreamLoad;
    private List<String> buffer = new ArrayList<>();

    public TableBufferedStreamLoadExecutor(int sinkMaxRetries, DorisFunction<RowData, String> valueTransform) {
        this.sinkMaxRetries = sinkMaxRetries;
        this.valueTransform = valueTransform;
    }

    @Override
    public void prepareDorisStreamLoad(DorisStreamLoad dorisStreamLoad) {
        this.dorisStreamLoad = dorisStreamLoad;
    }

    @Override
    public void addToBatch(RowData record) throws IOException {
        buffer.add(valueTransform.apply(record));
    }

    @Override
    public void executeBatch(DorisBackendSupplier<String> supplier) throws IOException {
        if (buffer.size() > 0) {
            dorisStreamLoad.execAndRetry(
                    sinkMaxRetries, dorisStreamLoad.concatData(buffer), false, supplier);
        }
    }
}
