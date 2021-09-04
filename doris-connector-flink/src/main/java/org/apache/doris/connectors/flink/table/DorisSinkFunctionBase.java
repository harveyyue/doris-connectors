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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_DEFAULT_CLUSTER;

public abstract class DorisSinkFunctionBase<IN> extends RichSinkFunction<IN> {

    protected DorisConfiguration dorisConfig;
    private LoadingCache<String, List<String>> backendCache;
    protected ObjectMapper objectMapper;

    public DorisSinkFunctionBase(DorisConfiguration dorisConfig) {
        this.dorisConfig = dorisConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        backendCache = CacheManager.buildBackendCache(dorisConfig, 1, TimeUnit.MINUTES);
        objectMapper = new ObjectMapper().configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
    }

    protected String getRandomBackend() throws IOException {
        List<String> backends = backendCache.get(DORIS_DEFAULT_CLUSTER);
        if (backends.size() == 0) {
            throw new IOException("No available be");
        }
        Collections.shuffle(backends);
        return backends.get(0);
    }
}
