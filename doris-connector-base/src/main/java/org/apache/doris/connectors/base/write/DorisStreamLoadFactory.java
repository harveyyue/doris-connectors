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

import org.apache.doris.connectors.base.cfg.DorisConfiguration;

import java.util.Locale;

public class DorisStreamLoadFactory {
    public static DorisStreamLoad instance(DorisConfiguration dorisConfig, String hostPort) {
        String db = dorisConfig.getDbName();
        String tbl = dorisConfig.getTableName();
        String user = dorisConfig.getUsername();
        String password = dorisConfig.getPassword();

        DorisStreamLoad dorisStreamLoad;
        switch (getDorisStreamLoadFormat(dorisConfig.getSinkFileFormatType())) {
            case JSON:
                dorisStreamLoad = new JsonDorisStreamLoad(hostPort, db, tbl, user, password);
                break;
            default:
                dorisStreamLoad = new CsvDorisStreamLoad(
                        hostPort,
                        db,
                        tbl,
                        user,
                        password,
                        dorisConfig.getSinkCsvColumnSeparator(),
                        dorisConfig.getSinkCsvLineDelimiter());
                break;
        }
        return dorisStreamLoad;
    }

    public static DorisStreamLoadFormat getDorisStreamLoadFormat(String format) {
        return DorisStreamLoadFormat.valueOf(format.toUpperCase(Locale.ROOT));
    }
}
