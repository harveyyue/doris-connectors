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

package org.apache.doris.connectors.flink.internal.options;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Optional;
import java.util.stream.Stream;

public class DorisDmlOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String tableName;
    private final String[] fieldNames;
    @Nullable
    private final String[] keyFields;

    public static DorisDmlOptionsBuilder builder() {
        return new DorisDmlOptionsBuilder();
    }

    private DorisDmlOptions(String tableName, String[] fieldNames, String[] keyFields) {
        this.tableName = tableName;
        this.fieldNames = fieldNames;
        this.keyFields = keyFields;
    }

    public String getTableName() {
        return tableName;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    @Nullable
    public Optional<String[]> getKeyFields() {
        return Optional.ofNullable(keyFields);
    }

    /**
     * Builder for {@link DorisDmlOptions}.
     */
    public static class DorisDmlOptionsBuilder {
        private String tableName;
        private String[] fieldNames;
        private String[] keyFields;

        public DorisDmlOptionsBuilder withFieldNames(String field, String... fieldNames) {
            this.fieldNames = concat(field, fieldNames);
            return this;
        }

        public DorisDmlOptionsBuilder withFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public DorisDmlOptionsBuilder withKeyFields(String keyField, String... keyFields) {
            this.keyFields = concat(keyField, keyFields);
            return this;
        }

        public DorisDmlOptionsBuilder withKeyFields(String[] keyFields) {
            this.keyFields = keyFields;
            return this;
        }

        public DorisDmlOptionsBuilder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public DorisDmlOptions build() {
            return new DorisDmlOptions(tableName, fieldNames, keyFields);
        }

        static String[] concat(String first, String... next) {
            if (next == null || next.length == 0) {
                return new String[]{first};
            } else {
                return Stream.concat(Stream.of(new String[]{first}), Stream.of(next))
                        .toArray(String[]::new);
            }
        }
    }
}

