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

package org.apache.doris.connectors.base.write;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_SINK_CSV_COLUMN_SEPARATOR_DEFAULT;
import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_SINK_CSV_LINE_DELIMITER_DEFAULT;

public class CsvDorisStreamLoad extends DorisStreamLoad {

    private String columnSeparator;
    private String lineDelimiter;

    public CsvDorisStreamLoad(String hostPort, String db, String tbl, String user, String password) {
        this(hostPort, db, tbl, user, password, DORIS_SINK_CSV_COLUMN_SEPARATOR_DEFAULT, DORIS_SINK_CSV_LINE_DELIMITER_DEFAULT);
    }

    public CsvDorisStreamLoad(
            String hostPort,
            String db,
            String tbl,
            String user,
            String password,
            String columnSeparator,
            String lineDelimiter) {
        super(hostPort, db, tbl, user, password);
        this.columnSeparator = columnSeparator;
        this.lineDelimiter = lineDelimiter;
    }

    @Override
    public Map<String, String> buildHeaderMap(String label, boolean isDeleteKind) {
        Map<String, String> headerMap = new HashMap<>();
        headerMap.put("Authorization", credential);
        headerMap.put("Expect", "100-continue");
        headerMap.put("label", label);
        headerMap.put("format", "csv");
        headerMap.put("column_separator", columnSeparator);
        headerMap.put("line_delimiter", lineDelimiter);
        if (isDeleteKind) {
            headerMap.put("merge_type", "delete");
        }
        return headerMap;
    }

    @Override
    public String concatData(List<String> data) {
        String values = data.stream().collect(Collectors.joining(lineDelimiter));
        if (!lineDelimiter.equalsIgnoreCase(DORIS_SINK_CSV_LINE_DELIMITER_DEFAULT)) {
            values += lineDelimiter;
        }
        return values;
    }
}
