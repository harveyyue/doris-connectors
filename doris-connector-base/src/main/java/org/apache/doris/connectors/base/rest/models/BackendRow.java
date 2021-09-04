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

package org.apache.doris.connectors.base.rest.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BackendRow implements Serializable {

    @JsonProperty(value = "HttpPort")
    private String httpPort;

    @JsonProperty(value = "IP")
    private String ip;

    @JsonProperty(value = "Alive")
    private Boolean alive;

    @JsonProperty(value = "DataUsedCapacity")
    private String dataUsedCapacity;

    @JsonProperty(value = "Cluster")
    private String cluster;

    public String getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(String httpPort) {
        this.httpPort = httpPort;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Boolean getAlive() {
        return alive;
    }

    public void setAlive(Boolean alive) {
        this.alive = alive;
    }

    public String getDataUsedCapacity() {
        return dataUsedCapacity;
    }

    public void setDataUsedCapacity(String dataUsedCapacity) {
        this.dataUsedCapacity = dataUsedCapacity;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    @Override
    public String toString() {
        return "BackendRow{" +
                "HttpPort='" + httpPort + '\'' +
                ", IP='" + ip + '\'' +
                ", Alive=" + alive +
                ", DataUsedCapacity='" + dataUsedCapacity + '\'' +
                ", Cluster='" + cluster + '\'' +
                '}';
    }
}
