/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.tso;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.omid.metrics.CodahaleMetricsConfig;
import org.apache.omid.metrics.CodahaleMetricsProvider;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.omid.timestamp.storage.DefaultHBaseTimestampStorageModule;
import org.apache.omid.committable.hbase.DefaultHBaseCommitTableStorageModule;
import com.google.inject.Module;

import org.apache.omid.NetworkUtils;
import org.apache.omid.YAMLUtils;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.tools.hbase.SecureHBaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static org.apache.omid.tls.X509Util.DEFAULT_PROTOCOL;

/**
 * Reads the configuration parameters of a TSO server instance from CONFIG_FILE_NAME.
 * If file CONFIG_FILE_NAME is missing defaults to DEFAULT_CONFIG_FILE_NAME
 */
@SuppressWarnings("all")
public class TSOServerConfig extends SecureHBaseConfig {

    private static final Logger LOG = LoggerFactory.getLogger(TSOServerConfig.class);

    private static final String CONFIG_FILE_NAME = "omid-server-configuration.yml";
    private static final String DEFAULT_CONFIG_FILE_NAME = "default-omid-server-configuration.yml";

    public static enum WAIT_STRATEGY {
        HIGH_THROUGHPUT,
        LOW_CPU
    };

    public static enum TIMESTAMP_TYPE {
      INCREMENTAL,
      WORLD_TIME
    };

    // ----------------------------------------------------------------------------------------------------------------
    // Instantiation
    // ----------------------------------------------------------------------------------------------------------------
    public TSOServerConfig() {
        this(CONFIG_FILE_NAME);
    }

    @VisibleForTesting
    TSOServerConfig(String configFileName) {
        Map props = new YAMLUtils().getSettingsMap(configFileName, DEFAULT_CONFIG_FILE_NAME);
        populateProperties(props);
    }

    public void populateProperties(Map props) {
        try {
            Object tsm;
            Map tsmProps = (Map) props.get("timestampStoreModule");
            if (tsmProps != null && tsmProps.containsKey("class") &&
                    "org.apache.omid.timestamp.storage.DefaultHBaseTimestampStorageModule".equals(tsmProps.get("class"))) {
                tsm = new DefaultHBaseTimestampStorageModule();
                tsmProps.remove("class");
            } else {
                tsm = new InMemoryTimestampStorageModule();
            }
            BeanUtils.populate((Object) tsm, (Map<String, ? extends Object>) tsmProps);
            props.put("timestampStoreModule", tsm);

            Object ctsm;
            Map ctsmProps = (Map) props.get("commitTableStoreModule");
            if (ctsmProps != null && ctsmProps.containsKey("class") &&
                "org.apache.omid.committable.hbase.DefaultHBaseCommitTableStorageModule".equals(ctsmProps.get("class"))) {
                ctsm = new DefaultHBaseCommitTableStorageModule();
                ctsmProps.remove("class");
            } else {
                ctsm = new InMemoryCommitTableStorageModule();
            }
            BeanUtils.populate((Object) ctsm, (Map<String, ? extends Object>) ctsmProps);
            props.put("commitTableStoreModule", ctsm);

            Object lmm;
            Map lmProps = (Map) props.get("leaseModule");
            if (lmProps != null && lmProps.containsKey("class") &&
                    "org.apache.omid.tso.HALeaseManagementModule".equals(lmProps.get("class"))) {
                lmm = new org.apache.omid.tso.HALeaseManagementModule();
                lmProps.remove("class");
            } else {
                lmm = new VoidLeaseManagementModule();
            }
            BeanUtils.populate((Object) lmm, (Map<String, ? extends Object>) lmProps);
            props.put("leaseModule", lmm);

            Object mp;
            Map mpProps = (Map) props.get("metrics");
            if (mpProps != null && mpProps.containsKey("class") &&
                    "org.apache.omid.metrics.CodahaleMetricsProvider".equals(mpProps.get("class"))) {
                CodahaleMetricsConfig mmc = new CodahaleMetricsConfig();
                mpProps.remove("class");
                Set reporters = new HashSet<>();
                for (Object r : (ArrayList) mpProps.get("reporters")) {
                    reporters.add(CodahaleMetricsConfig.Reporter.valueOf((String) r));
                }
                mpProps.put("reporters", reporters);
                BeanUtils.populate((Object) mmc, (Map<String, ? extends Object>) mpProps);
                mp = new CodahaleMetricsProvider(mmc);
            } else {
                mp = new NullMetricsProvider();
            }
            props.put("metrics", mp);

            BeanUtils.populate(this, props);
        } catch (IllegalAccessException | InvocationTargetException  | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Configuration parameters
    // ----------------------------------------------------------------------------------------------------------------

    private Module timestampStoreModule;

    private Module commitTableStoreModule;

    private Module leaseModule;

    private int port;

    private MetricsRegistry metrics;

    private int conflictMapSize;

    private int numConcurrentCTWriters;

    private int batchSizePerCTWriter;

    private int batchPersistTimeoutInMs;

    private String waitStrategy;

    private String networkIfaceName = NetworkUtils.getDefaultNetworkInterface();

    private String timestampType;

    private Boolean lowLatency;

    public boolean monitorContext;

    private boolean tlsEnabled = false;
    private boolean supportPlainText = true;

    private String keyStoreLocation = "";

    private String keyStorePassword = "";

    private String keyStoreType = "";

    private String trustStoreLocation = "";

    private String trustStorePassword = "";

    private String trustStoreType = "";

    private boolean sslCrlEnabled = false;

    private boolean sslOcspEnabled = false;

    private String enabledProtocols;

    private String cipherSuites;

    private String tlsConfigProtocols = DEFAULT_PROTOCOL;

    public boolean getMonitorContext() {
        return monitorContext;
    }

    public void setMonitorContext(boolean monitorContext) {
        this.monitorContext = monitorContext;
    }

    public Boolean getLowLatency() {
        return lowLatency;
    }

    public void setLowLatency(Boolean lowLatency) {
        this.lowLatency = lowLatency;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getConflictMapSize() {
        return conflictMapSize;
    }

    public void setConflictMapSize(int conflictMapSize) {
        this.conflictMapSize = conflictMapSize;
    }

    public int getNumConcurrentCTWriters() {
        return numConcurrentCTWriters;
    }

    public void setNumConcurrentCTWriters(int numConcurrentCTWriters) {
        this.numConcurrentCTWriters = numConcurrentCTWriters;
    }

    public int getBatchSizePerCTWriter() {
        return batchSizePerCTWriter;
    }

    public void setBatchSizePerCTWriter(int batchSizePerCTWriter) {
        this.batchSizePerCTWriter = batchSizePerCTWriter;
    }

    public int getBatchPersistTimeoutInMs() {
        return batchPersistTimeoutInMs;
    }

    public void setBatchPersistTimeoutInMs(int value) {
        this.batchPersistTimeoutInMs = value;
    }

    public String getNetworkIfaceName() {
        return networkIfaceName;
    }

    public void setNetworkIfaceName(String networkIfaceName) {
        this.networkIfaceName = networkIfaceName;
    }

    public String getTimestampType() {
        return timestampType;
    }

    public boolean getTlsEnabled() {
        return tlsEnabled;
    }

    public boolean getSupportPlainText() {
        return supportPlainText;
    }

    public String getKeyStoreLocation() {
        return keyStoreLocation;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public String getKeyStoreType() {
        return keyStoreType;
    }

    public String getTrustStoreLocation() {
        return trustStoreLocation;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public String getTrustStoreType() {
        return trustStoreType;
    }

    public boolean getSslCrlEnabled() {
        return sslCrlEnabled;
    }

    public boolean getSslOcspEnabled() {
        return sslOcspEnabled;
    }

    public String getEnabledProtocols() {
        return enabledProtocols;
    }

    public String getCipherSuites() {
        return cipherSuites;
    }

    public String getTsConfigProtocols() {
        return tlsConfigProtocols;
    }

    public void setTimestampType(String type) {
        this.timestampType = type;
    }

    public TIMESTAMP_TYPE getTimestampTypeEnum() {
        return TSOServerConfig.TIMESTAMP_TYPE.valueOf(timestampType);
    }

    public Module getTimestampStoreModule() {
        return timestampStoreModule;
    }

    public void setTimestampStoreModule(Module timestampStoreModule) {
        this.timestampStoreModule = timestampStoreModule;
    }

    public Module getCommitTableStoreModule() {
        return commitTableStoreModule;
    }

    public void setCommitTableStoreModule(Module commitTableStoreModule) {
        this.commitTableStoreModule = commitTableStoreModule;
    }

    public Module getLeaseModule() {
        return leaseModule;
    }

    public void setLeaseModule(Module leaseModule) {
        this.leaseModule = leaseModule;
    }

    public MetricsRegistry getMetrics() {
        return metrics;
    }

    public void setMetrics(MetricsRegistry metrics) {
        this.metrics = metrics;
    }

    public String getWaitStrategy() {
        return waitStrategy;
    }

    public WAIT_STRATEGY getWaitStrategyEnum() {
        return TSOServerConfig.WAIT_STRATEGY.valueOf(waitStrategy);
    }

    public void setWaitStrategy(String waitStrategy) {
        this.waitStrategy = waitStrategy;
    }

    public void setTlsEnabled(boolean tlsEnabled) {
        this.tlsEnabled = tlsEnabled;
    }

    public void setSupportPlainText(boolean supportPlainText) {
        this.supportPlainText = supportPlainText;
    }

    public void setKeyStoreLocation(String keyStoreLocation) {
        this.keyStoreLocation = keyStoreLocation;
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    public void setKeyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
    }

    public void setTrustStoreLocation(String trustStoreLocation) {
        this.trustStoreLocation = trustStoreLocation;
    }

    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }

    public void setTrustStoreType(String trustStoreType) {
        this.trustStoreType = trustStoreType;
    }

    public void setSslCrlEnabled(boolean sslCrlEnabled) {
        this.sslCrlEnabled = sslCrlEnabled;
    }

    public void setSslOcspEnabled(boolean sslOcspEnabled) {
        this.sslOcspEnabled = sslOcspEnabled;
    }

    public void setEnabledProtocols(String enabledProtocols) {
        this.enabledProtocols = enabledProtocols;
    }

    public void setCipherSuites(String cipherSuites) {
        this.cipherSuites = cipherSuites;
    }

    public void setTsConfigProtocols(String tlsConfigProtocols) {
        this.tlsConfigProtocols = tlsConfigProtocols;
    }
}
