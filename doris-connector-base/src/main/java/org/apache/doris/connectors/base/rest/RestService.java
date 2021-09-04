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

package org.apache.doris.connectors.base.rest;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.doris.connectors.base.cfg.DorisConfiguration;
import org.apache.doris.connectors.base.exception.DorisException;
import org.apache.doris.connectors.base.exception.IllegalArgumentException;
import org.apache.doris.connectors.base.exception.ShouldNeverHappenException;
import org.apache.doris.connectors.base.rest.models.Backend;
import org.apache.doris.connectors.base.rest.models.BackendRow;
import org.apache.doris.connectors.base.rest.models.DorisResponse;
import org.apache.doris.connectors.base.rest.models.PartitionDefinition;
import org.apache.doris.connectors.base.rest.models.QueryPlan;
import org.apache.doris.connectors.base.rest.models.Schema;
import org.apache.doris.connectors.base.rest.models.Tablet;
import org.apache.doris.connectors.base.util.OkHttpHelper;
import org.apache.doris.connectors.base.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_TABLET_SIZE;
import static org.apache.doris.connectors.base.cfg.ConfigurationOptions.DORIS_TABLET_SIZE_MIN;
import static org.apache.doris.connectors.base.util.ErrorMessages.ILLEGAL_ARGUMENT_MESSAGE;
import static org.apache.doris.connectors.base.util.ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE;

/**
 * Service for communicate with Doris FE.
 */
public class RestService implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(RestService.class);

    public static final int REST_RESPONSE_STATUS_OK = 200;
    private static final String TABLE_API_TEMPLATE = "http://%s/api/%s/%s/%s";
    private static final String SCHEMA = "_schema";
    private static final String QUERY_PLAN = "_query_plan";
    private static final String BACKENDS = "/rest/v1/system?path=//backends";
    private static ObjectMapper objectMapper;
    private static OkHttpClient okHttpClient;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        okHttpClient = OkHttpHelper.DEFAULT_OK_HTTP_CLIENT;
    }

    /**
     * choice a Doris FE node to request.
     *
     * @param feNodes Doris FE node list, separate be comma
     * @return the chosen one Doris FE node
     * @throws IllegalArgumentException fe nodes is illegal
     */
    static String randomEndpoint(String feNodes) throws IllegalArgumentException {
        LOG.trace("Parse fenodes '{}'.", feNodes);
        if (StringUtils.isEmpty(feNodes)) {
            LOG.error(ILLEGAL_ARGUMENT_MESSAGE, "fenodes", feNodes);
            throw new IllegalArgumentException("fenodes", feNodes);
        }
        List<String> nodes = Arrays.asList(feNodes.split(","));
        Collections.shuffle(nodes);
        return nodes.get(0).trim();
    }

    /**
     * choice a Doris BE node to request.
     *
     * @param dorisConfig configuration of request
     * @return the chosen one Doris BE node
     * @throws IllegalArgumentException BE nodes is illegal
     */
    public static String randomBackend(DorisConfiguration dorisConfig) throws DorisException {
        List<BackendRow> backends = getBackends(dorisConfig);
        LOG.trace("Parse beNodes '{}'.", backends);
        if (backends == null || backends.isEmpty()) {
            LOG.error(ILLEGAL_ARGUMENT_MESSAGE, "beNodes", backends);
            throw new IllegalArgumentException("beNodes", String.valueOf(backends));
        }
        Collections.shuffle(backends);
        BackendRow backend = backends.get(0);
        return backend.getIp() + ":" + backend.getHttpPort();
    }

    /**
     * get Doris BE nodes to request.
     *
     * @param dorisConfig configuration of request
     * @return the chosen one Doris BE node
     * @throws IllegalArgumentException BE nodes is illegal
     */
    public static List<BackendRow> getBackends(DorisConfiguration dorisConfig) throws DorisException {
        String feNodes = dorisConfig.getFenodes();
        String beUrl = "http://" + randomEndpoint(feNodes) + BACKENDS;
        try {
            Request request = new Request.Builder().url(beUrl).headers(buildHeaders(dorisConfig)).get().build();
            String body = okHttpClient.newCall(request).execute().body().string();
            LOG.debug("Backend Info: {}", body);
            DorisResponse<Backend> dorisResponse = getDorisResponse(body, Backend.class);
            List<BackendRow> backends =
                    dorisResponse.getData().getRows().stream().filter(v -> v.getAlive()).collect(Collectors.toList());
            return backends;
        } catch (IOException e) {
            throw new DorisException(e);
        }
    }

    /**
     * get a valid URI to connect Doris FE.
     *
     * @param dorisConfig configuration of request
     * @return uri string
     * @throws IllegalArgumentException throw when configuration is illegal
     */
    static String getUriStr(DorisConfiguration dorisConfig, String target) throws IllegalArgumentException {
        return String.format(TABLE_API_TEMPLATE,
                randomEndpoint(dorisConfig.getFenodes()), dorisConfig.getDbName(), dorisConfig.getTableName(), target);
    }

    /**
     * discover Doris table schema from Doris FE.
     *
     * @param dorisConfig configuration of request
     * @return Doris table schema
     * @throws DorisException throw when discover failed
     */
    public static Schema getSchema(DorisConfiguration dorisConfig) throws DorisException {
        String schemaUrl = getUriStr(dorisConfig, SCHEMA);
        DorisResponse<Schema> dorisResponse;
        try {
            Request request = new Request.Builder().url(schemaUrl).headers(buildHeaders(dorisConfig)).get().build();
            String body = okHttpClient.newCall(request).execute().body().string();
            LOG.debug("Find schema response is '{}'.", body);
            dorisResponse = getDorisResponse(body, Schema.class);
        } catch (IOException e) {
            throw new DorisException(e);
        }
        Schema schema = dorisResponse.getData();
        if (schema.getStatus() != REST_RESPONSE_STATUS_OK) {
            String errMsg = "Doris FE's response is not OK, status is " + schema.getStatus();
            LOG.error(errMsg);
            throw new DorisException(errMsg);
        }
        return schema;
    }

    /**
     * find Doris RDD partitions from Doris FE.
     *
     * @param dorisConfig configuration of request
     * @return an list of Doris RDD partitions
     * @throws DorisException throw when find partition failed
     */
    public static List<PartitionDefinition> findPartitions(DorisConfiguration dorisConfig) throws DorisException {
        String db = dorisConfig.getDbName();
        String table = dorisConfig.getTableName();
        String readFields = StringUtils.isBlank(dorisConfig.getReadFields()) ? "*" : dorisConfig.getReadFields();
        String sql = "select " + readFields + " from `" + db + "`.`" + table + "`";
        if (!StringUtils.isEmpty(dorisConfig.getFilterQuery())) {
            sql += " where " + dorisConfig.getFilterQuery();
        }
        String entity = "{\"sql\": \"" + sql + "\"}";
        LOG.debug("Query SQL Sending to Doris FE is: '{}'.", entity);
        String queryPlanUrl = getUriStr(dorisConfig, QUERY_PLAN);
        DorisResponse<QueryPlan> dorisResponse;

        try {
            Headers headers = buildHeaders(dorisConfig);
            RequestBody requestBody = buildRequestBody(entity);
            Request request = new Request.Builder().url(queryPlanUrl).headers(headers).post(requestBody).build();
            String body = okHttpClient.newCall(request).execute().body().string();
            LOG.debug("Find partition response is '{}'.", body);
            dorisResponse = getDorisResponse(body, QueryPlan.class);
        } catch (IOException e) {
            throw new DorisException(e);
        }
        QueryPlan queryPlan = dorisResponse.getData();
        if (queryPlan.getStatus() != REST_RESPONSE_STATUS_OK) {
            String errMsg = "Doris FE's response is not OK, status is " + queryPlan.getStatus();
            LOG.error(errMsg);
            throw new DorisException(errMsg);
        }
        Map<String, List<Long>> be2Tablets = selectBeForTablet(queryPlan);
        return tabletsMapToPartition(
                dorisConfig,
                be2Tablets,
                queryPlan.getOpaqued_query_plan(),
                db,
                table);
    }

    /**
     * execute doris sql
     *
     * @param dorisConfig configuration of request
     * @param sql         doris sql
     * @return map result
     * @throws DorisException
     * @throws IOException
     */
    public static Map<String, Object> executeSql(DorisConfiguration dorisConfig, String sql) throws DorisException {
        String queryUrl = "http://"
                + randomEndpoint(dorisConfig.getFenodes())
                + "/api/query/default_cluster/"
                + dorisConfig.getDbName();
        String value = "{\"stmt\": \"" + sql + "\"}";
        try {
            Headers headers = buildHeaders(dorisConfig);
            Request request =
                    new Request.Builder().url(queryUrl).headers(headers).post(buildRequestBody(value)).build();
            String body = okHttpClient.newCall(request).execute().body().string();
            DorisResponse<Map<String, Object>> ret = readValue(body, DorisResponse.class);
            return ret.getData();
        } catch (IOException e) {
            throw new DorisException(e);
        }
    }

    /**
     * select which Doris BE to get tablet data.
     *
     * @param queryPlan {@link QueryPlan} translated from Doris FE response
     * @return BE to tablets {@link Map}
     * @throws DorisException throw when select failed.
     */
    static Map<String, List<Long>> selectBeForTablet(QueryPlan queryPlan) throws DorisException {
        Map<String, List<Long>> be2Tablets = new HashMap<>();
        for (Map.Entry<String, Tablet> part : queryPlan.getPartitions().entrySet()) {
            LOG.debug("Parse tablet info: '{}'.", part);
            long tabletId;
            try {
                tabletId = Long.parseLong(part.getKey());
            } catch (NumberFormatException e) {
                String errMsg = "Parse tablet id '" + part.getKey() + "' to long failed.";
                LOG.error(errMsg, e);
                throw new DorisException(errMsg, e);
            }
            String target = null;
            int tabletCount = Integer.MAX_VALUE;
            for (String candidate : part.getValue().getRoutings()) {
                LOG.trace("Evaluate Doris BE '{}' to tablet '{}'.", candidate, tabletId);
                if (!be2Tablets.containsKey(candidate)) {
                    LOG.debug("Choice a new Doris BE '{}' for tablet '{}'.", candidate, tabletId);
                    List<Long> tablets = new ArrayList<>();
                    be2Tablets.put(candidate, tablets);
                    target = candidate;
                    break;
                } else {
                    if (be2Tablets.get(candidate).size() < tabletCount) {
                        target = candidate;
                        tabletCount = be2Tablets.get(candidate).size();
                        LOG.debug("Current candidate Doris BE to tablet '{}' is '{}' with tablet count {}.",
                                tabletId, target, tabletCount);
                    }
                }
            }
            if (target == null) {
                String errMsg = "Cannot choice Doris BE for tablet " + tabletId;
                LOG.error(errMsg);
                throw new DorisException(errMsg);
            }

            LOG.debug("Choice Doris BE '{}' for tablet '{}'.", target, tabletId);
            be2Tablets.get(target).add(tabletId);
        }
        return be2Tablets;
    }

    /**
     * tablet count limit for one Doris RDD partition
     *
     * @param dorisConfigs configuration of request
     * @return tablet count limit
     */
    static int tabletCountLimitForOnePartition(DorisConfiguration dorisConfigs) {
        int tabletsSize = dorisConfigs.getRequestTabletSize();
        if (tabletsSize < DORIS_TABLET_SIZE_MIN) {
            LOG.warn("{} is less than {}, set to default value {}.",
                    DORIS_TABLET_SIZE, DORIS_TABLET_SIZE_MIN, DORIS_TABLET_SIZE_MIN);
            tabletsSize = DORIS_TABLET_SIZE_MIN;
        }
        LOG.debug("Tablet size is set to {}.", tabletsSize);
        return tabletsSize;
    }

    /**
     * translate BE tablets map to Doris RDD partition.
     *
     * @param be2Tablets       BE to tablets {@link Map}
     * @param opaquedQueryPlan Doris BE execute plan getting from Doris FE
     * @param database         database name of Doris table
     * @param table            table name of Doris table
     * @return Doris table partition {@link List}
     * @throws IllegalArgumentException throw when translate failed
     */
    static List<PartitionDefinition> tabletsMapToPartition(
            DorisConfiguration dorisConfigs,
            Map<String, List<Long>> be2Tablets,
            String opaquedQueryPlan,
            String database,
            String table) {
        int tabletsSize = tabletCountLimitForOnePartition(dorisConfigs);
        List<PartitionDefinition> partitions = new ArrayList<>();
        for (Map.Entry<String, List<Long>> beInfo : be2Tablets.entrySet()) {
            LOG.debug("Generate partition with beInfo: '{}'.", beInfo);
            HashSet<Long> tabletSet = new HashSet<>(beInfo.getValue());
            beInfo.getValue().clear();
            beInfo.getValue().addAll(tabletSet);
            int first = 0;
            while (first < beInfo.getValue().size()) {
                Set<Long> partitionTablets = new HashSet<>(beInfo.getValue().subList(
                        first, Math.min(beInfo.getValue().size(), first + tabletsSize)));
                first = first + tabletsSize;
                PartitionDefinition partitionDefinition =
                        new PartitionDefinition(database, table, beInfo.getKey(), partitionTablets, opaquedQueryPlan);
                LOG.debug("Generate one PartitionDefinition '{}'.", partitionDefinition);
                partitions.add(partitionDefinition);
            }
        }
        return partitions;
    }

    static Headers buildHeaders(DorisConfiguration dorisConfig) {
        String credentials = OkHttpHelper.getCredentials(dorisConfig);
        return new Headers.Builder().add("Authorization", credentials).build();
    }

    static RequestBody buildRequestBody(String content) {
        return RequestBody.create(MediaType.parse("application/json;charset=UTF-8"), content);
    }

    static <T> DorisResponse<T> getDorisResponse(String body, Class genericParameter) throws DorisException {
        try {
            JavaType genericType =
                    objectMapper.getTypeFactory().constructParametricType(DorisResponse.class, genericParameter);
            return objectMapper.readValue(body, genericType);
        } catch (IOException e) {
            throw new DorisException(e);
        }
    }

    static <T> T readValue(String response, Class<T> valueType) throws DorisException {
        T obj;
        try {
            obj = objectMapper.readValue(response, valueType);
        } catch (JsonParseException e) {
            String errMsg = "Response is not a json. res: " + response;
            LOG.error(errMsg, e);
            throw new DorisException(errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "Response cannot map to schema. res: " + response;
            LOG.error(errMsg, e);
            throw new DorisException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse response to json failed. res: " + response;
            LOG.error(errMsg, e);
            throw new DorisException(errMsg, e);
        }
        if (obj == null) {
            LOG.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }
        return obj;
    }
}
