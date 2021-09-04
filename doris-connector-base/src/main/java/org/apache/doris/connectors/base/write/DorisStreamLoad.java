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

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Credentials;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.doris.connectors.base.exception.StreamLoadException;
import org.apache.doris.connectors.base.util.OkHttpHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_STREAM_LOAD_SUCCESS_STATUS;

public abstract class DorisStreamLoad {
    private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoad.class);

    private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load?";
    private static final String LOAD_LABEL_DATETIME_FORMAT = "yyyyMMdd_HHmmss";

    protected String hostPort;
    protected String db;
    protected String tbl;
    protected String credential;
    private DateTimeFormatter dateTimeFormatter;
    private ObjectMapper objectMapper;
    private OkHttpClient okHttpClient;

    public DorisStreamLoad(String hostPort, String db, String tbl, String user, String password) {
        this.hostPort = hostPort;
        this.db = db;
        this.tbl = tbl;
        this.credential = Credentials.basic(user, password);
        this.objectMapper = new ObjectMapper();
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(LOAD_LABEL_DATETIME_FORMAT);
        this.okHttpClient = OkHttpHelper.DEFAULT_OK_HTTP_CLIENT;
    }

    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
    }

    public StreamLoadContent load(String value, boolean isDeleteKind) throws StreamLoadException {
        StreamLoadContent streamLoadContent;
        String label = getLabel();
        String loadUrl = String.format(LOAD_URL_PATTERN, hostPort, db, tbl);
        Headers headers = Headers.of(buildHeaderMap(label, isDeleteKind));
        RequestBody body = RequestBody.create(MediaType.parse("application/json;charset=UTF-8"), value);
        Request request = new Request.Builder().url(loadUrl).headers(headers).put(body).build();

        LOG.info("Steamload BE: {}", loadUrl);
        try (Response response = okHttpClient.newCall(request).execute()) {
            if (response.isSuccessful()) {
                streamLoadContent = objectMapper.readValue(response.body().string(), StreamLoadContent.class);
                String contentStr = streamLoadContent.toString();
                String status = streamLoadContent.getStatus();
                if (DORIS_STREAM_LOAD_SUCCESS_STATUS.contains(status)) {
                    LOG.info("Stream load succeed: {}", contentStr);
                } else {
                    LOG.error("Stream load failed: {}", contentStr);
                }
            } else {
                throw new StreamLoadException(String.format("Stream load error: %d", response.code()));
            }
        } catch (IOException ex) {
            throw new StreamLoadException("Unexpected stream load error: " + ex);
        }

        return streamLoadContent;
    }

    public String getLabel() {
        return String.format("audit_%s_%s", dateTimeFormatter.format(LocalDateTime.now()),
                UUID.randomUUID().toString().replaceAll("-", ""));
    }

    public boolean execAndRetry(int retryTimes, String value, DorisBackendSupplier<String> supplier) throws IOException {
        return execAndRetry(retryTimes, value, false, supplier);
    }

    public boolean execAndRetry(
            int retryTimes, String value, boolean isDeleteKind, DorisBackendSupplier<String> supplier) throws IOException {
        StreamLoadContent content;
        int index = 0;
        while (true) {
            try {
                index += 1;
                content = load(value, isDeleteKind);
                if (DORIS_STREAM_LOAD_SUCCESS_STATUS.contains(content.getStatus())) {
                    return true;
                } else {
                    throw new StreamLoadException(String.format("Stream load failed: %s", content));
                }
            } catch (StreamLoadException ex) {
                LOG.error("Doris sink error, retry times = {}", index);
                if (index >= retryTimes) {
                    throw new IOException(String.format("Retry error, exceed %d times limit. %s", retryTimes, ex));
                }
                setHostPort(supplier.getBackend());
                try {
                    Thread.sleep(1000 * index);
                } catch (InterruptedException ie) {
                    throw new IOException("Interrupted error", ie);
                }
            }
        }
    }

    public abstract Map<String, String> buildHeaderMap(String label, boolean isDeleteKind);

    public abstract String concatData(List<String> data);

    public interface DorisBackendSupplier<T> extends Serializable {
        T getBackend() throws IOException;
    }

    public interface DorisFunction<T, R> extends Serializable {
        R apply(T t) throws IOException;
    }
}
