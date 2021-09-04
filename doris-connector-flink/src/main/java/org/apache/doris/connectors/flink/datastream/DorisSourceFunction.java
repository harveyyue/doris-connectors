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

package org.apache.doris.connectors.flink.datastream;

import org.apache.doris.connectors.base.cfg.DorisConfiguration;
import org.apache.doris.connectors.base.read.ScalaValueReader;
import org.apache.doris.connectors.base.rest.RestService;
import org.apache.doris.connectors.base.rest.models.PartitionDefinition;
import org.apache.doris.connectors.flink.deserialization.DorisDeserializationSchema;
import org.apache.doris.connectors.flink.serialization.FlinkRowBatch;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.compat.java8.functionConverterImpls.FromJavaBiFunction;

import java.util.List;

/**
 * DorisSource
 **/

public class DorisSourceFunction<T> extends RichSourceFunction<T> implements ResultTypeQueryable<T> {
    private static final Logger LOG = LoggerFactory.getLogger(DorisSourceFunction.class);

    private DorisConfiguration dorisConfig;
    private DorisDeserializationSchema deserializer;
    private List<PartitionDefinition> dorisPartitions;
    private ScalaValueReader scalaValueReader;

    public DorisSourceFunction(DorisConfiguration dorisConfig, DorisDeserializationSchema deserializer) {
        this.dorisConfig = dorisConfig;
        this.deserializer = deserializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.dorisPartitions = RestService.findPartitions(dorisConfig);
    }

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        for (PartitionDefinition partitions : dorisPartitions) {
            scalaValueReader = new ScalaValueReader(partitions, dorisConfig,
                    new FromJavaBiFunction<>((result, schema) -> new FlinkRowBatch(result, schema)));
            while (scalaValueReader.hasNext()) {
                Object next = scalaValueReader.next();
                sourceContext.collect(next);
            }
        }
    }

    @Override
    public void cancel() {
        scalaValueReader.close();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return this.deserializer.getProducedType();
    }
}
