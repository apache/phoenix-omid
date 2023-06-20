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
package org.apache.omid.tso.client;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.omid.YAMLUtils;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import static org.apache.omid.tls.X509Util.DEFAULT_HANDSHAKE_DETECTION_TIMEOUT_MILLIS;
import static org.apache.omid.tls.X509Util.DEFAULT_PROTOCOL;

/**
 * Configuration for Omid client side
 */
public class OmidClientConfiguration {

    private static final String DEFAULT_CONFIG_FILE_NAME = "omid-client-config.yml";

    public enum ConnType {DIRECT, HA}

    public enum PostCommitMode {SYNC, ASYNC}

    public enum ConflictDetectionLevel {CELL, ROW}

    // Basic connection related params

    private ConnType connectionType = ConnType.DIRECT;
    private String connectionString;
    private String zkCurrentTsoPath;
    private String zkNamespace;
    private int zkConnectionTimeoutInSecs;

    // Communication protocol related params

    private int requestMaxRetries;
    private int requestTimeoutInMs;
    private int reconnectionDelayInSecs;
    private int retryDelayInMs;
    private int executorThreads;

    // Transaction Manager related params

    private PostCommitMode postCommitMode = PostCommitMode.SYNC;
    private ConflictDetectionLevel conflictAnalysisLevel = ConflictDetectionLevel.CELL;

    private boolean tlsEnabled = false;

    private int clientNettyTlsHandshakeTimeout = DEFAULT_HANDSHAKE_DETECTION_TIMEOUT_MILLIS;

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

    // ----------------------------------------------------------------------------------------------------------------
    // Instantiation
    // ----------------------------------------------------------------------------------------------------------------

    public OmidClientConfiguration() {
        Map props = new YAMLUtils().getSettingsMap(DEFAULT_CONFIG_FILE_NAME);
        populateProperties(props);
    }

    @VisibleForTesting
    public OmidClientConfiguration(String configFileName) {
        Map props = new YAMLUtils().getSettingsMap(configFileName, DEFAULT_CONFIG_FILE_NAME);
        populateProperties(props);
    }

    public void populateProperties(Map props){
        try {
            if (props.containsKey("connectionType")) {
                props.put("connectionType", org.apache.omid.tso.client.OmidClientConfiguration.ConnType.valueOf((String) props.get("connectionType")));
            }
            if (props.containsKey("postCommitMode")) {
                props.put("postCommitMode", PostCommitMode.valueOf((String) props.get("postCommitMode")));
            }
            if (props.containsKey("conflictDetectionLevel")) {
                props.put("conflictDetectionLevel", ConflictDetectionLevel.valueOf((String) props.get("conflictDetectionLevel")));
            }
            BeanUtils.populate(this, props);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Getters and setters for config params
    // ----------------------------------------------------------------------------------------------------------------

    public ConnType getConnectionType() {
        return connectionType;
    }

    @Inject(optional = true)
    @Named("omid.client.connectionType")
    public void setConnectionType(ConnType connectionType) {
        this.connectionType = connectionType;
    }

    public String getConnectionString() {
        return connectionString;
    }

    @Inject(optional = true)
    @Named("omid.client.connectionString")
    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public int getZkConnectionTimeoutInSecs() {
        return zkConnectionTimeoutInSecs;
    }

    @Inject(optional = true)
    @Named("omid.client.zkConnectionTimeoutInSecs")
    public void setZkConnectionTimeoutInSecs(int zkConnectionTimeoutInSecs) {
        this.zkConnectionTimeoutInSecs = zkConnectionTimeoutInSecs;
    }

    public int getRequestMaxRetries() {
        return requestMaxRetries;
    }

    @Inject(optional = true)
    @Named("omid.client.requestMaxRetries")
    public void setRequestMaxRetries(int requestMaxRetries) {
        this.requestMaxRetries = requestMaxRetries;
    }

    public int getRequestTimeoutInMs() {
        return requestTimeoutInMs;
    }

    @Inject(optional = true)
    @Named("omid.client.requestTimeoutInMs")
    public void setRequestTimeoutInMs(int requestTimeoutInMs) {
        this.requestTimeoutInMs = requestTimeoutInMs;
    }

    public int getReconnectionDelayInSecs() {
        return reconnectionDelayInSecs;
    }

    @Inject(optional = true)
    @Named("omid.client.reconnectionDelayInSecs")
    public void setReconnectionDelayInSecs(int reconnectionDelayInSecs) {
        this.reconnectionDelayInSecs = reconnectionDelayInSecs;
    }

    public int getRetryDelayInMs() {
        return retryDelayInMs;
    }

    @Inject(optional = true)
    @Named("omid.client.retryDelayInMs")
    public void setRetryDelayInMs(int retryDelayInMs) {
        this.retryDelayInMs = retryDelayInMs;
    }

    public int getExecutorThreads() {
        return executorThreads;
    }

    @Inject(optional = true)
    @Named("omid.client.executorThreads")
    public void setExecutorThreads(int executorThreads) {
        this.executorThreads = executorThreads;
    }

    public String getZkCurrentTsoPath() {
        return zkCurrentTsoPath;
    }

    @Inject(optional = true)
    @Named("omid.ha.zkCurrentTsoPath")
    public void setZkCurrentTsoPath(String zkCurrentTsoPath) {
        this.zkCurrentTsoPath = zkCurrentTsoPath;
    }

    public String getZkNamespace() {
        return zkNamespace;
    }

    @Inject(optional = true)
    @Named("omid.ha.zkNamespace")
    public void setZkNamespace(String zkNamespace) {
        this.zkNamespace = zkNamespace;
    }

    public PostCommitMode getPostCommitMode() {
        return postCommitMode;
    }

    @Inject(optional = true)
    @Named("omid.tm.postCommitMode")
    public void setPostCommitMode(PostCommitMode postCommitMode) {
        this.postCommitMode = postCommitMode;
    }

    public ConflictDetectionLevel getConflictAnalysisLevel() {
        return conflictAnalysisLevel;
    }

    @Inject(optional = true)
    @Named("omid.tm.conflictAnalysisLevel")
    public void setConflictAnalysisLevel(ConflictDetectionLevel conflictAnalysisLevel) {
        this.conflictAnalysisLevel = conflictAnalysisLevel;
    }

    public boolean getTlsEnabled() {
        return tlsEnabled;
    }

    @Inject(optional = true)
    @Named("omid.client.tlsEnabled")
    public void setTlsEnabled(boolean tlsEnabled) {
        this.tlsEnabled = tlsEnabled;
    }

    public int getClientNettyTlsHandshakeTimeout() {
        return clientNettyTlsHandshakeTimeout;
    }

    @Inject(optional = true)
    @Named("omid.client.clientNettyTlsHandshakeTimeout")
    public void setClientNettyTlsHandshakeTimeout(int clientNettyTlsHandshakeTimeout) {
        this.clientNettyTlsHandshakeTimeout = clientNettyTlsHandshakeTimeout;
    }

    public String getKeyStoreLocation() {
        return keyStoreLocation;
    }

    @Inject(optional = true)
    @Named("omid.client.keyStoreLocation")
    public void setKeyStoreLocation(String keyStoreLocation) {
        this.keyStoreLocation = keyStoreLocation;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    @Inject(optional = true)
    @Named("omid.client.keyStorePassword")
    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    public String getKeyStoreType() {
        return keyStoreType;
    }

    @Inject(optional = true)
    @Named("omid.client.keyStoreType")

    public void setKeyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
    }

    public String getTrustStoreLocation() {
        return trustStoreLocation;
    }

    @Inject(optional = true)
    @Named("omid.client.trustStoreLocation")
    public void setTrustStoreLocation(String trustStoreLocation) {
        this.trustStoreLocation = trustStoreLocation;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    @Inject(optional = true)
    @Named("omid.client.trustStorePassword")
    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }

    public String getTrustStoreType() {
        return trustStoreType;
    }

    @Inject(optional = true)
    @Named("omid.client.trustStoreType")
    public void setTrustStoreType(String trustStoreType) {
        this.trustStoreType = trustStoreType;
    }

    public boolean getSslCrlEnabled() {
        return sslCrlEnabled;
    }

    @Inject(optional = true)
    @Named("omid.client.sslCrlEnabled")
    public void setSslCrlEnabled(boolean sslCrlEnabled) {
        this.sslCrlEnabled = sslCrlEnabled;
    }

    public boolean getSslOcspEnabled() {
        return sslOcspEnabled;
    }

    @Inject(optional = true)
    @Named("omid.client.sslCrlEnabled")
    public void setSslOcspEnabled(boolean sslOcspEnabled) {
        this.sslOcspEnabled = sslOcspEnabled;
    }

    public String getEnabledProtocols() {
        return enabledProtocols;
    }

    @Inject(optional = true)
    @Named("omid.client.enabledProtocols")
    public void setEnabledProtocols(String enabledProtocols) {
        this.enabledProtocols = enabledProtocols;
    }

    public String getCipherSuites() {
        return cipherSuites;
    }

    @Inject(optional = true)
    @Named("omid.client.cipherSuites")
    public void setCipherSuites(String cipherSuites) {
        this.cipherSuites = cipherSuites;
    }

    public String getTsConfigProtocols() {
        return tlsConfigProtocols;
    }

    @Inject(optional = true)
    @Named("omid.client.tlsConfigProtocols")
    public void setTsConfigProtocols(String tlsConfigProtocols) {
        this.tlsConfigProtocols = tlsConfigProtocols;
    }



}
