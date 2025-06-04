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
package org.apache.omid.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.client.ZKClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ZKUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ZKUtils.class);

    public static CuratorFramework initZKClient(String zkCluster, String namespace, int zkConnectionTimeoutInSec, String zkLoginContextName)
            throws IOException {

        LOG.info("Creating Zookeeper Client connecting to {}", zkCluster);

        ZKClientConfig zkConfig = new ZKClientConfig();
        if (zkLoginContextName != null) {
            // TODO should we check if this exists ?
            // Or just error out with an unsuccessful connection, as we do now ?
            LOG.info("Using Login Context {} for Zookeeper", zkCluster);
            zkConfig.setProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY, zkLoginContextName);
        }

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .namespace(namespace)
                .connectString(zkCluster)
                .retryPolicy(retryPolicy)
                .zkClientConfig(zkConfig)
                .build();

        zkClient.start();

        try {
            if (zkClient.blockUntilConnected(zkConnectionTimeoutInSec, TimeUnit.SECONDS)) {
                LOG.info("Connected to ZK cluster '{}', client in state: [{}]", zkCluster, zkClient.getState());
            } else {
                String errorMsg = String.format("Can't contact ZK cluster '%s' after %d seconds",
                                                zkCluster, zkConnectionTimeoutInSec);
                throw new IOException(errorMsg);
            }
        } catch (InterruptedException ex) {
            throw new IOException(String.format("Interrupted whilst connecting to ZK cluster '%s'", zkCluster));
        }

        return zkClient;

    }

}
