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

public class JsonDorisStreamLoad extends DorisStreamLoad {

    public JsonDorisStreamLoad(String hostPort, String db, String tbl, String user, String password) {
        super(hostPort, db, tbl, user, password);
    }

    @Override
    public Map<String, String> buildHeaderMap(String label, boolean isDeleteKind) {
        Map<String, String> headerMap = new HashMap<>();
        headerMap.put("Authorization", credential);
        headerMap.put("Expect", "100-continue");
        headerMap.put("label", label);
        headerMap.put("format", "json");
        headerMap.put("strip_outer_array", "true");
        if (isDeleteKind) {
            headerMap.put("merge_type", "delete");
        }
        return headerMap;
    }

    @Override
    public String concatData(List<String> data) {
        String values = data.stream().collect(Collectors.joining(","));
        return String.format("[%s]", values);
    }
}
