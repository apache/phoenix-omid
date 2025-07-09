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

import org.apache.omid.tls.X509Util;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TSOServerConfigTest {

    @Test(timeOut = 10_000)
    public void testParsesOK() throws Exception {
        TSOServerConfig tsoServerConfig = new TSOServerConfig("test-omid.yml");
        Assert.assertEquals(tsoServerConfig.getTlsEnabled(), false);
        Assert.assertEquals(tsoServerConfig.getSupportPlainText(), true);
        Assert.assertEquals(tsoServerConfig.getEnabledProtocols(), null);
        Assert.assertEquals(tsoServerConfig.getTsConfigProtocols(), X509Util.DEFAULT_PROTOCOLS);
        Assert.assertEquals(tsoServerConfig.getKeyStoreLocation(), "");
        Assert.assertEquals(tsoServerConfig.getKeyStorePassword(), "");
        Assert.assertEquals(tsoServerConfig.getKeyStoreType(), "");
        Assert.assertEquals(tsoServerConfig.getTrustStoreLocation(), "");
        Assert.assertEquals(tsoServerConfig.getTrustStorePassword(), "");
        Assert.assertEquals(tsoServerConfig.getKeyStoreType(), "");
    }

    @Test(timeOut = 10_000)
    public void testCustomParseK() throws Exception {
        TSOServerConfig tsoServerConfig = new TSOServerConfig("tlstest-omid-server-config.yml");
        Assert.assertEquals(tsoServerConfig.getCipherSuites(), "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA");
        Assert.assertEquals(tsoServerConfig.getTlsEnabled(), true);
        Assert.assertEquals(tsoServerConfig.getSupportPlainText(), false);

        Assert.assertEquals(tsoServerConfig.getEnabledProtocols(), "TLSv1.2");
        Assert.assertEquals(tsoServerConfig.getTsConfigProtocols(), X509Util.DEFAULT_PROTOCOLS);

        Assert.assertEquals(tsoServerConfig.getKeyStoreLocation(), "/asd");
        Assert.assertEquals(tsoServerConfig.getKeyStorePassword(), "pass");
        Assert.assertEquals(tsoServerConfig.getKeyStoreType(), "JKS");
        Assert.assertEquals(tsoServerConfig.getTrustStoreLocation(), "/tasd");
        Assert.assertEquals(tsoServerConfig.getTrustStorePassword(), "tpas$w0rd1");
        Assert.assertEquals(tsoServerConfig.getKeyStoreType(), "JKS");

    }


}