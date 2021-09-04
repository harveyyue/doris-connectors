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
import org.apache.doris.connectors.base.exception.DorisException;
import org.apache.doris.connectors.base.rest.RestService;
import org.apache.doris.connectors.base.rest.models.PartitionDefinition;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * The {@link DorisDynamicTableSource} is used during planning.
 *
 * <p>In our example, we don't implement any of the available ability interfaces such as {@link SupportsFilterPushDown}
 * or {@link SupportsProjectionPushDown}. Therefore, the main logic can be found in {@link #getScanRuntimeProvider(ScanContext)}
 * where we instantiate the required {@link SourceFunction} and its {@link DeserializationSchema} for
 * runtime. Both instances are parameterized to return internal data structures (i.e. {@link RowData}).
 */
public final class DorisDynamicTableSource implements ScanTableSource, LookupTableSource {
    private static final Logger LOG = LoggerFactory.getLogger(DorisRowDataInputFormat.class);

    private final DorisConfiguration dorisConfig;
    private TableSchema physicalSchema;

    public DorisDynamicTableSource(DorisConfiguration dorisConfig, TableSchema physicalSchema) {
        this.dorisConfig = dorisConfig;
        this.physicalSchema = physicalSchema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // in our example the format decides about the changelog mode
        // but it could also be the source itself
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        List<PartitionDefinition> dorisPartitions;
        try {
            dorisPartitions = RestService.findPartitions(dorisConfig);
        } catch (DorisException e) {
            throw new RuntimeException("can not fetch partitions");
        }
        DorisRowDataInputFormat dorisRowDataInputFormat = new DorisRowDataInputFormat(dorisConfig, dorisPartitions);
        return InputFormatProvider.of(dorisRowDataInputFormat);
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        return null;
    }

    @Override
    public DynamicTableSource copy() {
        return new DorisDynamicTableSource(dorisConfig, physicalSchema);
    }

    @Override
    public String asSummaryString() {
        return "Doris Table Source";
    }
}
