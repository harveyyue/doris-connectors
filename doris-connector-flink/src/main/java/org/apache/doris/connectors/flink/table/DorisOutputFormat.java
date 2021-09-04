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

import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.doris.connectors.base.cfg.DorisConfiguration;
import org.apache.doris.connectors.base.util.CacheManager;
import org.apache.doris.connectors.base.write.DorisStreamLoad;
import org.apache.doris.connectors.base.write.DorisStreamLoadFactory;
import org.apache.doris.connectors.flink.internal.executor.DorisStreamLoadExecutor;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_DEFAULT_CLUSTER;

/**
 * DorisDynamicOutputFormat
 **/
public class DorisOutputFormat<IN, DorisExec extends DorisStreamLoadExecutor<IN>>
        extends RichOutputFormat<IN> implements Flushable {

    private static final Logger LOG = LoggerFactory.getLogger(DorisOutputFormat.class);

    private final DorisConfiguration dorisConfig;
    private final StreamLoadExecutorFactory<DorisExec> streamLoadExecutorFactory;

    private DorisStreamLoad dorisStreamLoad;
    private transient volatile boolean closed = false;
    private LoadingCache<String, List<String>> backendCache;

    private transient DorisExec dorisStreamLoadExecutor;
    private transient int batchCount = 0;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile Exception flushException;

    public DorisOutputFormat(
            DorisConfiguration dorisConfig, StreamLoadExecutorFactory streamLoadExecutorFactory) {
        this.dorisConfig = dorisConfig;
        this.streamLoadExecutorFactory = streamLoadExecutorFactory;
    }

    @Override
    public void configure(Configuration configuration) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        backendCache = CacheManager.buildBackendCache(dorisConfig, 1, TimeUnit.MINUTES);
        dorisStreamLoad = DorisStreamLoadFactory.instance(dorisConfig, getRandomBackend());

        dorisStreamLoadExecutor = streamLoadExecutorFactory.get();
        dorisStreamLoadExecutor.prepareDorisStreamLoad(dorisStreamLoad);

        if (dorisConfig.getSinkBatchIntervalMs() != 0 && dorisConfig.getSinkBatchSize() != 1) {
            this.scheduler = Executors.newScheduledThreadPool(
                    1, new ExecutorThreadFactory("doris-upsert-output-format"));
            this.scheduledFuture = scheduler.scheduleWithFixedDelay(
                    () -> {
                        synchronized (DorisOutputFormat.class) {
                            if (!closed) {
                                try {
                                    flush();
                                } catch (Exception ex) {
                                    flushException = ex;
                                }
                            }
                        }
                    },
                    dorisConfig.getSinkBatchIntervalMs(),
                    dorisConfig.getSinkBatchIntervalMs(),
                    TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void writeRecord(IN record) throws IOException {
        checkFlushException();

        try {
            dorisStreamLoadExecutor.addToBatch(record);
            batchCount++;
            if (dorisConfig.getSinkBatchSize() > 0 && batchCount >= dorisConfig.getSinkBatchSize()) {
                flush();
            }
        } catch (Exception e) {
            throw new IOException("Writing records to Doris failed.", e);
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;

            if (this.scheduledFuture != null) {
                this.scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            if (batchCount > 0) {
                try {
                    flush();
                } catch (Exception e) {
                    LOG.warn("Writing records to doris failed.", e);
                    throw new RuntimeException("Writing records to doris failed.", e);
                }
            }

            checkFlushException();
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        checkFlushException();
        dorisStreamLoadExecutor.executeBatch(() -> getRandomBackend());
        batchCount = 0;
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to Doris failed.", flushException);
        }
    }

    /**
     * Get random doris backend instance for stream load
     */
    public String getRandomBackend() throws IOException {
        List<String> backends = backendCache.get(DORIS_DEFAULT_CLUSTER);
        if (backends.size() == 0) {
            throw new IOException("No available be");
        }
        Collections.shuffle(backends);
        return backends.get(0);
    }

    public interface StreamLoadExecutorFactory<T extends DorisStreamLoadExecutor<?>> extends Supplier<T>, Serializable {
    }
}
