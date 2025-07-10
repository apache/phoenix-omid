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

package org.apache.omid.tls;


import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.security.Security;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * This file has is based on the one in HBase project.
 * @see <a href=
 *      "https://github.com/apache/hbase/blob/d2b0074f7ad4c43d31a1a511a0d74feda72451d1/hbase-common/src/test/java/org/apache/hadoop/hbase/io/crypto/tls/TestX509Util.java">Base
 *      revision</a>
 */
@RunWith(Parameterized.class)
public class TestX509Util extends BaseX509ParameterizedTestCase {

    @Parameterized.Parameter()
    public X509KeyType caKeyType;

    @Parameterized.Parameter(value = 1)
    public X509KeyType certKeyType;

    @Parameterized.Parameter(value = 2)
    public String keyPassword;

    @Parameterized.Parameter(value = 3)
    public Integer paramIndex;

    @Parameterized.Parameters(
            name = "{index}: caKeyType={0}, certKeyType={1}, keyPassword={2}, paramIndex={3}")
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();
        int paramIndex = 0;
        for (X509KeyType caKeyType : X509KeyType.values()) {
            for (X509KeyType certKeyType : X509KeyType.values()) {
                for (String keyPassword : new String[] { KEY_EMPTY_PASSWORD, KEY_NON_EMPTY_PASSWORD }) {
                    params.add(new Object[] { caKeyType, certKeyType, keyPassword, paramIndex++ });
                }
            }
        }
        return params;
    }

    private String tlsConfigKeystoreLocation;
    private String tlsConfigKeystorePassword;
    private String tlsConfigKeystoreType;

    private String tlsConfigTrustLocation;
    private String tlsConfigTrustPassword;
    private String tlsConfigTrustType;

    @Override
    public void init(X509KeyType caKeyType, X509KeyType certKeyType, String keyPassword,
                     Integer paramIndex) throws Exception {
        super.init(caKeyType, certKeyType, keyPassword, paramIndex);
        x509TestContext.setSystemProperties(KeyStoreFileType.JKS, KeyStoreFileType.JKS);
        tlsConfigKeystoreLocation = x509TestContext.getTlsConfigKeystoreLocation();
        tlsConfigKeystorePassword = x509TestContext.getTlsConfigKeystorePassword();
        tlsConfigKeystoreType = x509TestContext.getTlsConfigKeystoreType();
        tlsConfigTrustLocation = x509TestContext.getTlsConfigTrustLocation();
        tlsConfigTrustPassword = x509TestContext.getTlsConfigTrustPassword();
        tlsConfigTrustType = x509TestContext.getTlsConfigTrustType();
    }

    @After
    public void cleanUp() {
        x509TestContext.clearSystemProperties();
        System.clearProperty("com.sun.net.ssl.checkRevocation");
        System.clearProperty("com.sun.security.enableCRLDP");
        Security.setProperty("ocsp.enable", Boolean.FALSE.toString());
        Security.setProperty("com.sun.security.enableCRLDP", Boolean.FALSE.toString());
    }

    @Test
    public void testCreateSSLContextWithoutCustomProtocol() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        SslContext sslContext = X509Util.createSslContextForClient(tlsConfigKeystoreLocation, tlsConfigKeystorePassword.toCharArray(), tlsConfigKeystoreType, tlsConfigTrustLocation, tlsConfigTrustPassword.toCharArray(), tlsConfigTrustType, false, false, null, null, X509Util.DEFAULT_PROTOCOLS);
        ByteBufAllocator byteBufAllocatorMock = mock(ByteBufAllocator.class);
        assertEquals(X509Util.DEFAULT_PROTOCOLS.split(","),
                sslContext.newEngine(byteBufAllocatorMock).getEnabledProtocols());
    }

    @Test
    public void testCreateSSLContextWithCustomProtocol() throws Exception {
        final String protocol = "TLSv1.1";
        init(caKeyType, certKeyType, keyPassword, paramIndex);

        ByteBufAllocator byteBufAllocatorMock = mock(ByteBufAllocator.class);
        SslContext sslContext = X509Util.createSslContextForClient(tlsConfigKeystoreLocation, tlsConfigKeystorePassword.toCharArray(), tlsConfigKeystoreType, tlsConfigTrustLocation, tlsConfigTrustPassword.toCharArray(), tlsConfigTrustType, false, false, protocol, null, X509Util.DEFAULT_PROTOCOLS);
        assertEquals(Collections.singletonList(protocol),
                Arrays.asList(sslContext.newEngine(byteBufAllocatorMock).getEnabledProtocols()));
    }

    @Test(expected = X509Exception.SSLContextException.class)
    public void testCreateSSLContextWithoutKeyStoreLocationServer() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        tlsConfigKeystoreLocation = "";
        SslContext sslContext = X509Util.createSslContextForServer(tlsConfigKeystoreLocation, tlsConfigKeystorePassword.toCharArray(), tlsConfigKeystoreType, tlsConfigTrustLocation, tlsConfigTrustPassword.toCharArray(), tlsConfigTrustType, false, false, null, null, X509Util.DEFAULT_PROTOCOLS);
    }

    @Test
    public void testCreateSSLContextWithoutKeyStoreLocationClient() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        tlsConfigKeystoreLocation = "";
        SslContext sslContext = X509Util.createSslContextForClient(tlsConfigKeystoreLocation, tlsConfigKeystorePassword.toCharArray(), tlsConfigKeystoreType, tlsConfigTrustLocation, tlsConfigTrustPassword.toCharArray(), tlsConfigTrustType, false, false, null, null, X509Util.DEFAULT_PROTOCOLS);
    }

    @Test(expected = X509Exception.class)
    public void testCreateSSLContextWithoutKeyStorePassword() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        if (!x509TestContext.isKeyStoreEncrypted()) {
            throw new X509Exception.SSLContextException("");
        }
        tlsConfigKeystorePassword = "";
        SslContext sslContext = X509Util.createSslContextForServer(tlsConfigKeystoreLocation, tlsConfigKeystorePassword.toCharArray(), tlsConfigKeystoreType, tlsConfigTrustLocation, tlsConfigTrustPassword.toCharArray(), tlsConfigTrustType, false, false, null, null, X509Util.DEFAULT_PROTOCOLS);
    }

    @Test
    public void testCreateSSLContextWithoutTrustStoreLocationClient() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        tlsConfigTrustLocation = "";
        SslContext sslContext = X509Util.createSslContextForClient(tlsConfigKeystoreLocation, tlsConfigKeystorePassword.toCharArray(), tlsConfigKeystoreType, tlsConfigTrustLocation, tlsConfigTrustPassword.toCharArray(), tlsConfigTrustType, false, false, null, null, X509Util.DEFAULT_PROTOCOLS);
    }

    @Test
    public void testCreateSSLContextWithoutTrustStoreLocationServer() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        tlsConfigTrustLocation = "";
        SslContext sslContext = X509Util.createSslContextForServer(tlsConfigKeystoreLocation, tlsConfigKeystorePassword.toCharArray(), tlsConfigKeystoreType, tlsConfigTrustLocation, tlsConfigTrustPassword.toCharArray(), tlsConfigTrustType, false, false, null, null, X509Util.DEFAULT_PROTOCOLS);
    }

    // It would be great to test the value of PKIXBuilderParameters#setRevocationEnabled,
    // but it does not appear to be possible
    @Test
    public void testCRLEnabled() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        SslContext sslContext = X509Util.createSslContextForServer(tlsConfigKeystoreLocation, tlsConfigKeystorePassword.toCharArray(), tlsConfigKeystoreType, tlsConfigTrustLocation, tlsConfigTrustPassword.toCharArray(), tlsConfigTrustType, true, false, null, null, X509Util.DEFAULT_PROTOCOLS);
        assertTrue(Boolean.valueOf(System.getProperty("com.sun.net.ssl.checkRevocation")));
        assertTrue(Boolean.valueOf(System.getProperty("com.sun.security.enableCRLDP")));
        assertFalse(Boolean.valueOf(Security.getProperty("ocsp.enable")));
    }

    @Test
    public void testCRLDisabled() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        SslContext sslContext = X509Util.createSslContextForServer(tlsConfigKeystoreLocation, tlsConfigKeystorePassword.toCharArray(), tlsConfigKeystoreType, tlsConfigTrustLocation, tlsConfigTrustPassword.toCharArray(), tlsConfigTrustType, false, false, null, null, X509Util.DEFAULT_PROTOCOLS);
        assertFalse(Boolean.valueOf(System.getProperty("com.sun.net.ssl.checkRevocation")));
        assertFalse(Boolean.valueOf(System.getProperty("com.sun.security.enableCRLDP")));
        assertFalse(Boolean.valueOf(Security.getProperty("ocsp.enable")));
    }

    @Test
    public void testLoadJKSKeyStore() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        // Make sure we can instantiate a key manager from the JKS file on disk
        X509Util.createKeyManager(
                x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
                x509TestContext.getKeyStorePassword().toCharArray(), KeyStoreFileType.JKS.getPropertyValue());
    }

    @Test
    public void testLoadJKSKeyStoreNullPassword() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        if (!x509TestContext.getKeyStorePassword().isEmpty()) {
            return;
        }
        // Make sure that empty password and null password are treated the same
        X509Util.createKeyManager(
                x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(), null,
                KeyStoreFileType.JKS.getPropertyValue());
    }

    @Test
    public void testLoadJKSKeyStoreFileTypeDefaultToJks() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        // Make sure we can instantiate a key manager from the JKS file on disk
        X509Util.createKeyManager(
                x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
                x509TestContext.getKeyStorePassword().toCharArray(),
                null /* null StoreFileType means 'autodetect from file extension' */);
    }

    @Test
    public void testLoadJKSKeyStoreWithWrongPassword() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        assertThrows(X509Exception.KeyManagerException.class, () -> {
            // Attempting to load with the wrong key password should fail
            X509Util.createKeyManager(
                    x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(), "wrong password".toCharArray(),
                    KeyStoreFileType.JKS.getPropertyValue());
        });
    }

    @Test
    public void testLoadJKSTrustStore() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        // Make sure we can instantiate a trust manager from the JKS file on disk
        X509Util.createTrustManager(
                x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
                x509TestContext.getTrustStorePassword().toCharArray(), KeyStoreFileType.JKS.getPropertyValue(), true, true);
    }

    @Test
    public void testLoadJKSTrustStoreNullPassword() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        if (!x509TestContext.getTrustStorePassword().isEmpty()) {
            return;
        }
        // Make sure that empty password and null password are treated the same
        X509Util.createTrustManager(
                x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(), null,
                KeyStoreFileType.JKS.getPropertyValue(), false, false);
    }

    @Test
    public void testLoadJKSTrustStoreFileTypeDefaultToJks() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        // Make sure we can instantiate a trust manager from the JKS file on disk
        X509Util.createTrustManager(
                x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
                x509TestContext.getTrustStorePassword().toCharArray(), null, // null StoreFileType means 'autodetect from
                // file extension'
                true, true);
    }

    @Test
    public void testLoadJKSTrustStoreWithWrongPassword() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        assertThrows(X509Exception.TrustManagerException.class, () -> {
            // Attempting to load with the wrong key password should fail
            X509Util.createTrustManager(
                    x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(), "wrong password".toCharArray(),
                    KeyStoreFileType.JKS.getPropertyValue(), true, true);
        });
    }

    @Test
    public void testLoadPKCS12KeyStore() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        // Make sure we can instantiate a key manager from the PKCS12 file on disk
        X509Util.createKeyManager(
                x509TestContext.getKeyStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
                x509TestContext.getKeyStorePassword().toCharArray(), KeyStoreFileType.PKCS12.getPropertyValue());
    }

    @Test
    public void testLoadPKCS12KeyStoreNullPassword() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        if (!x509TestContext.getKeyStorePassword().isEmpty()) {
            return;
        }
        // Make sure that empty password and null password are treated the same
        X509Util.createKeyManager(
                x509TestContext.getKeyStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(), null,
                KeyStoreFileType.PKCS12.getPropertyValue());
    }

    @Test
    public void testLoadPKCS12KeyStoreWithWrongPassword() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        assertThrows(X509Exception.KeyManagerException.class, () -> {
            // Attempting to load with the wrong key password should fail
            X509Util.createKeyManager(
                    x509TestContext.getKeyStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
                    "wrong password".toCharArray(), KeyStoreFileType.PKCS12.getPropertyValue());
        });
    }

    @Test
    public void testLoadPKCS12TrustStore() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        // Make sure we can instantiate a trust manager from the PKCS12 file on disk
        X509Util.createTrustManager(
                x509TestContext.getTrustStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
                x509TestContext.getTrustStorePassword().toCharArray(), KeyStoreFileType.PKCS12.getPropertyValue(), true,
                true);
    }

    @Test
    public void testLoadPKCS12TrustStoreNullPassword() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        if (!x509TestContext.getTrustStorePassword().isEmpty()) {
            return;
        }
        // Make sure that empty password and null password are treated the same
        X509Util.createTrustManager(
                x509TestContext.getTrustStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(), null,
                KeyStoreFileType.PKCS12.getPropertyValue(), false, false);
    }

    @Test
    public void testLoadPKCS12TrustStoreWithWrongPassword() throws Exception {
        init(caKeyType, certKeyType, keyPassword, paramIndex);
        assertThrows(X509Exception.TrustManagerException.class, () -> {
            // Attempting to load with the wrong key password should fail
            X509Util.createTrustManager(
                    x509TestContext.getTrustStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
                    "wrong password".toCharArray(), KeyStoreFileType.PKCS12.getPropertyValue(), true, true);
        });
    }

}
