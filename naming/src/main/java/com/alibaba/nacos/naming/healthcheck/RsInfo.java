/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.naming.healthcheck;

import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;

import java.util.Map;

/**
 * Metrics info of server
 *
 * @author nacos
 */
public class RsInfo {
    private String datumKey = null;
    private double load;
    private double cpu;
    private double rt;
    private double qps;
    private double mem;
    private int port;
    private String ip;
    private String serviceName;
    private String ak;
    private String cluster;
    private double weight;
    private boolean ephemeral = true;
    private Map<String, String> metadata;

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getAk() {
        return ak;
    }

    public void setAk(String ak) {
        this.ak = ak;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public double getLoad() {
        return load;
    }

    public void setLoad(double load) {
        this.load = load;
    }

    public double getCpu() {
        return cpu;
    }

    public void setCpu(double cpu) {
        this.cpu = cpu;
    }

    public double getRt() {
        return rt;
    }

    public void setRt(double rt) {
        this.rt = rt;
    }

    public double getQps() {
        return qps;
    }

    public void setQps(double qps) {
        this.qps = qps;
    }

    public double getMem() {
        return mem;
    }

    public void setMem(double mem) {
        this.mem = mem;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public boolean isEphemeral() {
        return ephemeral;
    }

    public void setEphemeral(boolean ephemeral) {
        this.ephemeral = ephemeral;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return JacksonUtils.toJson(this);
    }

    public String getDatumKey() {
        if (StringUtils.isBlank(datumKey)) {
            if (getPort() > 0) {
                datumKey = getIp() + ":" + getPort() + ":" + UtilsAndCommons.LOCALHOST_SITE + ":" + getCluster();
            }
            else {
                datumKey = getIp() + ":" + UtilsAndCommons.LOCALHOST_SITE + ":" + getCluster();
            }
        }
        return datumKey;
    }

    public static RsInfoBuilder builder() {
        return new RsInfoBuilder();
    }

    public static final class RsInfoBuilder {
        private String datumKey = null;
        private double load;
        private double cpu;
        private double rt;
        private double qps;
        private double mem;
        private int port;
        private String ip;
        private String serviceName;
        private String ak;
        private String cluster;
        private double weight;
        private boolean ephemeral = true;
        private Map<String, String> metadata;

        private RsInfoBuilder() {
        }

        public RsInfoBuilder datumKey(String datumKey) {
            this.datumKey = datumKey;
            return this;
        }

        public RsInfoBuilder load(double load) {
            this.load = load;
            return this;
        }

        public RsInfoBuilder cpu(double cpu) {
            this.cpu = cpu;
            return this;
        }

        public RsInfoBuilder rt(double rt) {
            this.rt = rt;
            return this;
        }

        public RsInfoBuilder qps(double qps) {
            this.qps = qps;
            return this;
        }

        public RsInfoBuilder mem(double mem) {
            this.mem = mem;
            return this;
        }

        public RsInfoBuilder port(int port) {
            this.port = port;
            return this;
        }

        public RsInfoBuilder ip(String ip) {
            this.ip = ip;
            return this;
        }

        public RsInfoBuilder serviceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public RsInfoBuilder ak(String ak) {
            this.ak = ak;
            return this;
        }

        public RsInfoBuilder cluster(String cluster) {
            this.cluster = cluster;
            return this;
        }

        public RsInfoBuilder weight(double weight) {
            this.weight = weight;
            return this;
        }

        public RsInfoBuilder ephemeral(boolean ephemeral) {
            this.ephemeral = ephemeral;
            return this;
        }

        public RsInfoBuilder metadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public RsInfo build() {
            RsInfo rsInfo = new RsInfo();
            rsInfo.setLoad(load);
            rsInfo.setCpu(cpu);
            rsInfo.setRt(rt);
            rsInfo.setQps(qps);
            rsInfo.setMem(mem);
            rsInfo.setPort(port);
            rsInfo.setIp(ip);
            rsInfo.setServiceName(serviceName);
            rsInfo.setAk(ak);
            rsInfo.setCluster(cluster);
            rsInfo.setWeight(weight);
            rsInfo.setEphemeral(ephemeral);
            rsInfo.setMetadata(metadata);
            rsInfo.datumKey = this.datumKey;
            return rsInfo;
        }
    }
}
