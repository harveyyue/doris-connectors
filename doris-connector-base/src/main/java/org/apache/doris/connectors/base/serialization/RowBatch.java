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

package org.apache.doris.connectors.base.serialization;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.apache.doris.connectors.base.exception.DorisException;
import org.apache.doris.connectors.base.rest.models.Schema;
import org.apache.doris.thrift.TScanBatchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * row batch data container.
 */
public abstract class RowBatch {
    private static Logger LOG = LoggerFactory.getLogger(RowBatch.class);

    public static class Row {
        private List<Object> cols;

        Row(int colCount) {
            this.cols = new ArrayList<>(colCount);
        }

        public List<Object> getCols() {
            return cols;
        }

        public void put(Object o) {
            cols.add(o);
        }
    }

    // offset for iterate the rowBatch
    private int offsetInRowBatch;
    protected int rowCountInOneBatch = 0;
    private int readRowCount = 0;
    private List<Row> rowBatch = new ArrayList<>();
    private final ArrowStreamReader arrowStreamReader;
    private VectorSchemaRoot root;
    protected List<FieldVector> fieldVectors;
    private RootAllocator rootAllocator;
    protected final Schema schema;

    public RowBatch(TScanBatchResult nextResult, Schema schema) {
        this.schema = schema;
        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        this.arrowStreamReader = new ArrowStreamReader(new ByteArrayInputStream(nextResult.getRows()), rootAllocator);
        this.offsetInRowBatch = 0;
    }

    public List<Row> getRowBatch() {
        return rowBatch;
    }

    public RowBatch readArrow() throws DorisException {
        try {
            this.root = arrowStreamReader.getVectorSchemaRoot();
            while (arrowStreamReader.loadNextBatch()) {
                fieldVectors = root.getFieldVectors();
                if (fieldVectors.size() != schema.size()) {
                    LOG.error("Schema size '{}' is not equal to arrow field size '{}'.",
                            fieldVectors.size(), schema.size());
                    throw new DorisException("Load Doris data failed, schema size of fetch data is wrong.");
                }
                if (fieldVectors.size() == 0 || root.getRowCount() == 0) {
                    LOG.debug("One batch in arrow has no data.");
                    continue;
                }
                rowCountInOneBatch = root.getRowCount();
                // init the rowBatch
                for (int i = 0; i < rowCountInOneBatch; ++i) {
                    rowBatch.add(new Row(fieldVectors.size()));
                }
                convertArrowToRowBatch();
                readRowCount += root.getRowCount();
            }
            return this;
        } catch (Exception e) {
            LOG.error("Read Doris Data failed because: ", e);
            throw new DorisException(e.getMessage());
        } finally {
            close();
        }
    }

    protected void addValueToRow(int rowIndex, Object obj) {
        if (rowIndex > rowCountInOneBatch) {
            String errMsg = "Get row offset: " + rowIndex + " larger than row size: " +
                    rowCountInOneBatch;
            LOG.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        rowBatch.get(readRowCount + rowIndex).put(obj);
    }

    public abstract void convertArrowToRowBatch() throws DorisException;

    public boolean hasNext() {
        if (offsetInRowBatch < readRowCount) {
            return true;
        }
        return false;
    }

    public List<Object> next() {
        if (!hasNext()) {
            String errMsg = "Get row offset:" + offsetInRowBatch + " larger than row size: " + readRowCount;
            LOG.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        return rowBatch.get(offsetInRowBatch++).getCols();
    }

    protected String typeMismatchMessage(final String sparkType, final Types.MinorType arrowType) {
        final String messageTemplate = "FLINK type is %1$s, but arrow type is %2$s.";
        return String.format(messageTemplate, sparkType, arrowType.name());
    }

    public int getReadRowCount() {
        return readRowCount;
    }

    public void close() {
        try {
            if (arrowStreamReader != null) {
                arrowStreamReader.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
        } catch (IOException ioe) {
            LOG.error(ioe.toString());
        }
    }
}
