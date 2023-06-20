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
package org.apache.omid.transaction;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.omid.YAMLUtils;
import org.apache.omid.metrics.CodahaleMetricsConfig;
import org.apache.omid.metrics.CodahaleMetricsProvider;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.tools.hbase.SecureHBaseConfig;
import org.apache.omid.tso.client.OmidClientConfiguration.ConflictDetectionLevel;
import org.apache.omid.tso.client.OmidClientConfiguration.PostCommitMode;
import org.apache.omid.tso.client.OmidClientConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Configuration for HBase's Omid client side
 */
public class HBaseOmidClientConfiguration extends SecureHBaseConfig {

    private static final String DEFAULT_CONFIG_FILE_NAME = "default-hbase-omid-client-config.yml";
    private static final String CONFIG_FILE_NAME = "hbase-omid-client-config.yml";
    private Configuration hbaseConfiguration = HBaseConfiguration.create();
    private String commitTableName;
    @Inject
    private OmidClientConfiguration omidClientConfiguration;
    private MetricsRegistry metrics;

    // ----------------------------------------------------------------------------------------------------------------
    // Instantiation
    // ----------------------------------------------------------------------------------------------------------------
    
    public static HBaseOmidClientConfiguration loadFromString(String yamlContent) {
        return new HBaseOmidClientConfiguration(new YAMLUtils().loadStringAsMap(yamlContent));
    }

    public HBaseOmidClientConfiguration() {
        this(CONFIG_FILE_NAME);
    }

    public <K, V> HBaseOmidClientConfiguration(Map<String, ? extends Object> properties) {
        try {
            BeanUtils.populate(this, properties);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

    @VisibleForTesting
    HBaseOmidClientConfiguration(String configFileName) {
        Map props = new YAMLUtils().getSettingsMap(configFileName, DEFAULT_CONFIG_FILE_NAME);
        populateProperties(props);
    }

    public void populateProperties(Map props) {
        try {
            if (props.containsKey("omidClientConfiguration")) {
                Object omidClientConf = new OmidClientConfiguration();
                Map omidClientProps = (Map<String, ? extends Object>) props.get("omidClientConfiguration");
                if (omidClientProps != null && omidClientProps.containsKey("connectionType")) {
                    omidClientProps.put("connectionType", org.apache.omid.tso.client.OmidClientConfiguration.ConnType.valueOf((String) omidClientProps.get("connectionType")));
                }
                if (omidClientProps != null && omidClientProps.containsKey("postCommitMode")) {
                    omidClientProps.put("postCommitMode", PostCommitMode.valueOf((String) omidClientProps.get("postCommitMode")));
                }
                if (omidClientProps != null && omidClientProps.containsKey("conflictDetectionLevel")) {
                    omidClientProps.put("conflictDetectionLevel", ConflictDetectionLevel.valueOf((String) omidClientProps.get("conflictDetectionLevel")));
                }
                BeanUtils.populate((Object) omidClientConf, omidClientProps);
                props.put("omidClientConfiguration", omidClientConf);
            }

            Object mp;
            Map mpProps = (Map) props.get("metrics");
            System.out.println(mpProps);
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
        } catch (IllegalAccessException | InvocationTargetException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Getters and setters for config params
    // ----------------------------------------------------------------------------------------------------------------

    public Configuration getHBaseConfiguration() {
        return hbaseConfiguration;
    }

    public void setHBaseConfiguration(Configuration hbaseConfiguration) {
        this.hbaseConfiguration = hbaseConfiguration;
    }

    public PostCommitMode getPostCommitMode() {
        return omidClientConfiguration.getPostCommitMode();
    }

    public void setPostCommitMode(PostCommitMode postCommitMode) {
        omidClientConfiguration.setPostCommitMode(postCommitMode);
    }

    public ConflictDetectionLevel getConflictAnalysisLevel() {
        return omidClientConfiguration.getConflictAnalysisLevel();
    }

    public void setConflictAnalysisLevel(ConflictDetectionLevel conflictAnalysisLevel) {
        omidClientConfiguration.setConflictAnalysisLevel(conflictAnalysisLevel);
    }

    public String getCommitTableName() {
        return commitTableName;
    }

    @Inject(optional = true)
    @Named("omid.client.hbase.commitTableName")
    public void setCommitTableName(String commitTableName) {
        this.commitTableName = commitTableName;
    }

    public OmidClientConfiguration getOmidClientConfiguration() {
        return omidClientConfiguration;
    }

    public void setOmidClientConfiguration(OmidClientConfiguration omidClientConfiguration) {
        this.omidClientConfiguration = omidClientConfiguration;
    }

    public MetricsRegistry getMetrics() {
        return metrics;
    }

    @Inject(optional = true)
    @Named("omid.client.hbase.metrics")
    public void setMetrics(MetricsRegistry metrics) {
        this.metrics = metrics;
    }

    // Delegation to make end-user life better

    public OmidClientConfiguration.ConnType getConnectionType() {
        return omidClientConfiguration.getConnectionType();
    }

    public void setReconnectionDelayInSecs(int reconnectionDelayInSecs) {
        omidClientConfiguration.setReconnectionDelayInSecs(reconnectionDelayInSecs);
    }

    public void setExecutorThreads(int executorThreads) {
        omidClientConfiguration.setExecutorThreads(executorThreads);
    }

    public int getRequestTimeoutInMs() {
        return omidClientConfiguration.getRequestTimeoutInMs();
    }

    public void setConnectionString(String connectionString) {
        omidClientConfiguration.setConnectionString(connectionString);
    }

    public void setRequestTimeoutInMs(int requestTimeoutInMs) {
        omidClientConfiguration.setRequestTimeoutInMs(requestTimeoutInMs);
    }

    public void setZkConnectionTimeoutInSecs(int zkConnectionTimeoutInSecs) {
        omidClientConfiguration.setZkConnectionTimeoutInSecs(zkConnectionTimeoutInSecs);
    }

    public void setConnectionType(OmidClientConfiguration.ConnType connectionType) {
        omidClientConfiguration.setConnectionType(connectionType);
    }

    public void setRequestMaxRetries(int requestMaxRetries) {
        omidClientConfiguration.setRequestMaxRetries(requestMaxRetries);
    }

    public int getZkConnectionTimeoutInSecs() {
        return omidClientConfiguration.getZkConnectionTimeoutInSecs();
    }

    public void setRetryDelayInMs(int retryDelayInMs) {
        omidClientConfiguration.setRetryDelayInMs(retryDelayInMs);
    }

    public int getExecutorThreads() {
        return omidClientConfiguration.getExecutorThreads();
    }

    public int getRetryDelayInMs() {
        return omidClientConfiguration.getRetryDelayInMs();
    }

    public String getConnectionString() {
        return omidClientConfiguration.getConnectionString();
    }

    public int getRequestMaxRetries() {
        return omidClientConfiguration.getRequestMaxRetries();
    }

    public int getReconnectionDelayInSecs() {
        return omidClientConfiguration.getReconnectionDelayInSecs();
    }

}
