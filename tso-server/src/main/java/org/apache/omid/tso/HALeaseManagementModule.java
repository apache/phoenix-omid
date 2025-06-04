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

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.curator.framework.CuratorFramework;
import org.apache.omid.timestamp.storage.ZKModule;
import org.apache.omid.tso.LeaseManagement.LeaseManagementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import javax.inject.Singleton;

import static org.apache.omid.tso.TSOServer.TSO_HOST_AND_PORT_KEY;

public class HALeaseManagementModule extends AbstractModule {

    private static final Logger LOG = LoggerFactory.getLogger(HALeaseManagementModule.class);

    private long leasePeriodInMs = 10_000; // 10 secs
    private String tsoLeasePath = "/tso-lease";
    private String currentTsoPath = "/current-tso";
    private String zkCluster = "localhost:2181";
    private String zkNamespace = "omid";
    private String zkLoginContextName;

    // ----------------------------------------------------------------------------------------------------------------
    // WARNING: Do not remove empty constructor, needed by snake_yaml!
    // ----------------------------------------------------------------------------------------------------------------

    public HALeaseManagementModule() {

    }

    @VisibleForTesting
    public HALeaseManagementModule(long leasePeriodInMs, String tsoLeasePath, String currentTsoPath,
                                   String zkCluster, String zkNamespace, String zkLoginContextName) {

        this.leasePeriodInMs = leasePeriodInMs;
        this.tsoLeasePath = tsoLeasePath;
        this.currentTsoPath = currentTsoPath;
        this.zkCluster = zkCluster;
        this.zkNamespace = zkNamespace;
        this.zkLoginContextName = zkLoginContextName;

    }

    @Override
    protected void configure() {
        install(new ZKModule(zkCluster, zkNamespace, zkLoginContextName));

    }

    @Provides
    @Singleton
    LeaseManagement provideLeaseManager(@Named(TSO_HOST_AND_PORT_KEY) String tsoHostAndPort,
                                        TSOChannelHandler tsoChannelHandler,
                                        TSOStateManager stateManager,
                                        CuratorFramework zkClient,
                                        Panicker panicker) throws LeaseManagementException {

        LOG.info("Connection to HA cluster [{}]", zkClient.getState());

        return new LeaseManager(tsoHostAndPort,
                                tsoChannelHandler,
                                stateManager,
                                leasePeriodInMs,
                                tsoLeasePath,
                                currentTsoPath,
                                zkClient,
                                panicker);

    }

    // ----------------------------------------------------------------------------------------------------------------
    // WARNING: Do not remove getters/setters, needed by snake_yaml!
    // ----------------------------------------------------------------------------------------------------------------

    public String getCurrentTsoPath() {
        return currentTsoPath;
    }

    public void setCurrentTsoPath(String currentTsoPath) {
        this.currentTsoPath = currentTsoPath;
    }

    public long getLeasePeriodInMs() {
        return leasePeriodInMs;
    }

    public void setLeasePeriodInMs(long leasePeriodInMs) {
        this.leasePeriodInMs = leasePeriodInMs;
    }

    public String getTsoLeasePath() {
        return tsoLeasePath;
    }

    public void setTsoLeasePath(String tsoLeasePath) {
        this.tsoLeasePath = tsoLeasePath;
    }

    public String getZkCluster() {
        return zkCluster;
    }

    public void setZkCluster(String zkCluster) {
        this.zkCluster = zkCluster;
    }

    public String getZkNamespace() {
        return zkNamespace;
    }

    public void setZkNamespace(String zkNamespace) {
        this.zkNamespace = zkNamespace;
    }

    public String getZkLoginContextName() {
        return zkLoginContextName;
    }

    public void setZkLoginContextName(String zkLoginContextName) {
        this.zkLoginContextName = zkLoginContextName;
    }

}
