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

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import org.apache.omid.tls.X509KeyType;
import org.apache.omid.tls.X509TestContext;
import org.apache.omid.tls.X509Util;
import org.apache.omid.tls.KeyStoreFileType;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.security.Security;

import static org.mockito.Mockito.mock;

public class TestGetSslContext {
    private static final String CURRENT_TSO_PATH = "/current_tso_path";

    @Test(timeOut = 10_000)
    public void testYamlReading() throws Exception {
        OmidClientConfiguration tsoClientConf = new OmidClientConfiguration();
        tsoClientConf.setConnectionString("localhost:" + 1234);
        tsoClientConf.setZkCurrentTsoPath(CURRENT_TSO_PATH);
        tsoClientConf.setTlsEnabled(true);

        Security.addProvider(new BouncyCastleProvider());
        File tempDir = Files.createTempDirectory("x509Tests").toFile();
        String keyPassword = "pa$$w0rd";
        X509KeyType certKeyType = X509KeyType.RSA;
        X509KeyType caKeyType = X509KeyType.RSA;

        X509TestContext x509TestContext = X509TestContext.newBuilder().setTempDir(tempDir).setKeyStorePassword(keyPassword)
                .setKeyStoreKeyType(certKeyType).setTrustStorePassword(keyPassword)
                .setTrustStoreKeyType(caKeyType).build();

        x509TestContext.setSystemProperties(KeyStoreFileType.JKS, KeyStoreFileType.JKS);

        tsoClientConf.setKeyStoreLocation(x509TestContext.getTlsConfigKeystoreLocation());;
        tsoClientConf.setKeyStorePassword(x509TestContext.getTlsConfigKeystorePassword());
        tsoClientConf.setKeyStoreType(x509TestContext.getTlsConfigKeystoreType());
        tsoClientConf.setTrustStoreLocation(x509TestContext.getTlsConfigTrustLocation());
        tsoClientConf.setTrustStorePassword(x509TestContext.getTlsConfigTrustPassword());
        tsoClientConf.setTrustStoreType(x509TestContext.getTlsConfigTrustType());
        String cipherSuite = "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA";
        tsoClientConf.setCipherSuites(cipherSuite);

        TSOClient tsoClient = TSOClient.newInstance(tsoClientConf);
        SslContext sslContext = tsoClient.getSslContext(tsoClientConf);

        ByteBufAllocator byteBufAllocatorMock = mock(ByteBufAllocator.class);
        Assert.assertEquals(new String[] { X509Util.DEFAULT_PROTOCOL },
                sslContext.newEngine(byteBufAllocatorMock).getEnabledProtocols());

        Assert.assertEquals(new String[] { cipherSuite },
                sslContext.newEngine(byteBufAllocatorMock).getEnabledCipherSuites());

        Assert.assertFalse(Boolean.valueOf(System.getProperty("com.sun.net.ssl.checkRevocation")));
        Assert.assertFalse(Boolean.valueOf(System.getProperty("com.sun.security.enableCRLDP")));
        Assert.assertFalse(Boolean.valueOf(Security.getProperty("ocsp.enable")));

    }

}