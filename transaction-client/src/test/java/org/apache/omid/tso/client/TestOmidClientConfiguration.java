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

import org.testng.Assert;
import org.testng.annotations.Test;
import org.apache.omid.tso.client.OmidClientConfiguration.ConnType;
import org.apache.omid.tls.X509Util;
import org.apache.omid.tso.client.OmidClientConfiguration.ConflictDetectionLevel;

public class TestOmidClientConfiguration {

    @Test(timeOut = 10_000)
    public void testYamlReading() {
        OmidClientConfiguration configuration = new OmidClientConfiguration();
        Assert.assertEquals(configuration.getConnectionString(), "localhost:24758");
        Assert.assertEquals(configuration.getConnectionType(), ConnType.DIRECT);
        Assert.assertEquals(configuration.getEnabledProtocols(), null);
        Assert.assertEquals(configuration.getTsConfigProtocols(), X509Util.DEFAULT_PROTOCOLS);
        Assert.assertEquals(configuration.getTlsEnabled(), false);
        Assert.assertEquals(configuration.getKeyStoreLocation(), "");
        Assert.assertEquals(configuration.getKeyStorePassword(), "");
        Assert.assertEquals(configuration.getTrustStoreLocation(), "");
        Assert.assertEquals(configuration.getTrustStorePassword(), "");
        Assert.assertEquals(configuration.getKeyStoreType(), "");
        Assert.assertEquals(configuration.getKeyStoreType(), "");
    }

    @Test(timeOut = 10_000)
    public void testCustomYamlReading() {
        OmidClientConfiguration configuration = new OmidClientConfiguration("tlstest-omid-client-config.yml");
        Assert.assertEquals(configuration.getConnectionString(), "localhost:24758");
        Assert.assertEquals(configuration.getConnectionType(), ConnType.DIRECT);
        Assert.assertEquals(configuration.getEnabledProtocols(), "TLSv1.2");
        Assert.assertEquals(configuration.getTsConfigProtocols(), X509Util.DEFAULT_PROTOCOLS);
        Assert.assertEquals(configuration.getTlsEnabled(), true);
        Assert.assertEquals(configuration.getKeyStoreLocation(), "/asd");
        Assert.assertEquals(configuration.getKeyStorePassword(), "pass");

        Assert.assertEquals(configuration.getZkConnectionTimeoutInSecs(), 10);
        Assert.assertEquals(configuration.getZkNamespace(), "omid");
        Assert.assertEquals(configuration.getZkCurrentTsoPath(), "/current-tso");

        Assert.assertEquals(configuration.getRequestMaxRetries(), 5);
        Assert.assertEquals(configuration.getRequestTimeoutInMs(), 5000);
        Assert.assertEquals(configuration.getReconnectionDelayInSecs(), 10);
        Assert.assertEquals(configuration.getRetryDelayInMs(), 1000);
        Assert.assertEquals(configuration.getExecutorThreads(), 3);

        Assert.assertEquals(configuration.getPostCommitMode(), OmidClientConfiguration.PostCommitMode.SYNC);
        Assert.assertEquals(configuration.getConflictAnalysisLevel(), OmidClientConfiguration.ConflictDetectionLevel.CELL);

        Assert.assertEquals(configuration.getKeyStoreType(), "JKS");
        Assert.assertEquals(configuration.getTrustStoreLocation(), "/tasd");
        Assert.assertEquals(configuration.getTrustStorePassword(), "tpas$w0rd1");
        Assert.assertEquals(configuration.getKeyStoreType(), "JKS");
        Assert.assertEquals(configuration.getCipherSuites(), "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA");
    }

}