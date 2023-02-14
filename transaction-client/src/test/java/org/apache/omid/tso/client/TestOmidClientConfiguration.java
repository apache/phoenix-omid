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

public class TestOmidClientConfiguration {

    @Test(timeOut = 10_000)
    public void testYamlReading() {
        OmidClientConfiguration configuration = new OmidClientConfiguration();
        Assert.assertNotNull(configuration.getConnectionString());
        Assert.assertNotNull(configuration.getConnectionType());
        Assert.assertEquals(configuration.getEnabledProtocols(), null);
        Assert.assertEquals(configuration.getTsConfigProtocols(), "TLSv1.2");
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
        OmidClientConfiguration configuration = new OmidClientConfiguration("custom-omid-client-config.yml");
        Assert.assertNotNull(configuration.getConnectionString());
        Assert.assertNotNull(configuration.getConnectionType());
        Assert.assertEquals(configuration.getEnabledProtocols(), "TLSv1.2");
        Assert.assertEquals(configuration.getTsConfigProtocols(), "TLSv1.2");
        Assert.assertEquals(configuration.getTlsEnabled(), true);
        Assert.assertEquals(configuration.getKeyStoreLocation(), "/asd");
        Assert.assertEquals(configuration.getKeyStorePassword(), "pass");

        Assert.assertEquals(configuration.getKeyStoreType(), "JKS");
        Assert.assertEquals(configuration.getTrustStoreLocation(), "/tasd");
        Assert.assertEquals(configuration.getTrustStorePassword(), "tpas$w0rd1");
        Assert.assertEquals(configuration.getKeyStoreType(), "JKS");
        Assert.assertEquals(configuration.getCipherSuites(), "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA");
    }

}