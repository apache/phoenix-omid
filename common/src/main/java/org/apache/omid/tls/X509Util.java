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

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.apache.omid.tls.X509Exception.KeyManagerException;
import org.apache.omid.tls.X509Exception.SSLContextException;
import org.apache.omid.tls.X509Exception.TrustManagerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.X509CertSelector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * Utility code for X509 handling Default cipher suites.
 * <p/>
 * This file has is based on the one in HBase project, which is based in the one in Zookeeper.
 * @see <a href=
 *      "https://github.com/apache/hbase/blob/d2b0074f7ad4c43d31a1a511a0d74feda72451d1/hbase-common/src/main/java/org/apache/hadoop/hbase/io/crypto/tls/X509Util.java">Base
 *      revision</a>
 */
public final class X509Util {

    private static final Logger LOG = LoggerFactory.getLogger(X509Util.class);
    private static final char[] EMPTY_CHAR_ARRAY = new char[0];

    public static final String TLS_1_2 = "TLSv1.2";
    public static final String TLS_1_3 = "TLSv1.3";

    // Config
    public static final int DEFAULT_HANDSHAKE_DETECTION_TIMEOUT_MILLIS = 5000;

    public static final String DEFAULT_PROTOCOLS = defaultTlsProtocols();

    private X509Util() {
        // disabled
    }

    private static String defaultTlsProtocols() {
        String defaultProtocol = TLS_1_2;
        List<String> supported = new ArrayList<>();
        try {
            supported = Arrays.asList(SSLContext.getDefault().getSupportedSSLParameters().getProtocols());
            if (supported.contains(TLS_1_3)) {
                defaultProtocol = TLS_1_3 + "," + TLS_1_2;
            }
        } catch (NoSuchAlgorithmException e) {
            // Ignore.
        }
        LOG.info("Default TLS protocols are {}, supported TLS protocols are {}", defaultProtocol, supported);
        return defaultProtocol;
    }

    public static SslContext createSslContextForClient(String keyStoreLocation, char[] keyStorePassword,
                                                       String keyStoreType, String trustStoreLocation, char[] trustStorePassword, String trustStoreType,
                                                       boolean sslCrlEnabled, boolean sslOcspEnabled, String enabledProtocols, String cipherSuites, String tlsConfigProtocols)
            throws X509Exception, IOException {

        SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();

        if (keyStoreLocation.isEmpty()) {
            LOG.warn("keyStoreLocation is not specified");
        } else {
            sslContextBuilder
                    .keyManager(createKeyManager(keyStoreLocation, keyStorePassword, keyStoreType));
        }

        if (trustStoreLocation.isEmpty()) {
            LOG.warn("trustStoreLocation is not specified");
        } else {
            sslContextBuilder.trustManager(createTrustManager(trustStoreLocation, trustStorePassword,
                    trustStoreType, sslCrlEnabled, sslOcspEnabled));
        }

        sslContextBuilder.enableOcsp(sslOcspEnabled);
        sslContextBuilder.protocols(getEnabledProtocols(enabledProtocols, tlsConfigProtocols));
        if (cipherSuites != null && !cipherSuites.isEmpty()) {
            sslContextBuilder.ciphers(Arrays.asList(cipherSuites.split(",")));
        }

        return sslContextBuilder.build();
    }

    public static SslContext createSslContextForServer(String keyStoreLocation, char[] keyStorePassword,
      String keyStoreType, String trustStoreLocation, char[] trustStorePassword, String trustStoreType,
      boolean sslCrlEnabled, boolean sslOcspEnabled, String enabledProtocols, String cipherSuites, String tlsConfigProtocols)
            throws X509Exception, IOException {

        if (keyStoreLocation.isEmpty()) {
            throw new SSLContextException(
                    "keyStoreLocation is required for SSL server: ");
        }

        SslContextBuilder sslContextBuilder;

        sslContextBuilder = SslContextBuilder
                .forServer(createKeyManager(keyStoreLocation, keyStorePassword, keyStoreType));

        if (trustStoreLocation.isEmpty()) {
            LOG.warn("trustStoreLocation is not specified");
        } else {
            sslContextBuilder.trustManager(createTrustManager(trustStoreLocation, trustStorePassword,
                    trustStoreType, sslCrlEnabled, sslOcspEnabled));
        }

        sslContextBuilder.enableOcsp(sslOcspEnabled);
        sslContextBuilder.protocols(getEnabledProtocols(enabledProtocols, tlsConfigProtocols));
        if (cipherSuites != null && !cipherSuites.isEmpty()) {
            sslContextBuilder.ciphers(Arrays.asList(cipherSuites.split(",")));
        }

        return sslContextBuilder.build();
    }

    /**
     * Creates a key manager by loading the key store from the given file of the given type,
     * optionally decrypting it using the given password.
     * @param keyStoreLocation the location of the key store file.
     * @param keyStorePassword optional password to decrypt the key store. If empty, assumes the key
     *                         store is not encrypted.
     * @param keyStoreType     must be JKS, PEM, PKCS12, BCFKS or null. If null, attempts to
     *                         autodetect the key store type from the file extension (e.g. .jks /
     *                         .pem).
     * @return the key manager.
     * @throws KeyManagerException if something goes wrong.
     */
    static X509KeyManager createKeyManager(String keyStoreLocation, char[] keyStorePassword,
                                           String keyStoreType) throws KeyManagerException {

        if (keyStoreType == null) {
            keyStoreType = "jks";
        }

        if (keyStorePassword == null) {
            keyStorePassword = EMPTY_CHAR_ARRAY;
        }

        try {
            KeyStore ks = KeyStore.getInstance(keyStoreType);
            try (InputStream inputStream = Files.newInputStream(new File(keyStoreLocation).toPath())) {
                ks.load(inputStream, keyStorePassword);
            }

            KeyManagerFactory kmf = KeyManagerFactory.getInstance("PKIX");
            kmf.init(ks, keyStorePassword);

            for (KeyManager km : kmf.getKeyManagers()) {
                if (km instanceof X509KeyManager) {
                    return (X509KeyManager) km;
                }
            }
            throw new KeyManagerException("Couldn't find X509KeyManager");
        } catch (IOException | GeneralSecurityException | IllegalArgumentException e) {
            throw new KeyManagerException(e);
        }
    }

    /**
     * Creates a trust manager by loading the trust store from the given file of the given type,
     * optionally decrypting it using the given password.
     * @param trustStoreLocation the location of the trust store file.
     * @param trustStorePassword optional password to decrypt the trust store (only applies to JKS
     *                           trust stores). If empty, assumes the trust store is not encrypted.
     * @param trustStoreType     must be JKS, PEM, PKCS12, BCFKS or null. If null, attempts to
     *                           autodetect the trust store type from the file extension (e.g. .jks /
     *                           .pem).
     * @param crlEnabled         enable CRL (certificate revocation list) checks.
     * @param ocspEnabled        enable OCSP (online certificate status protocol) checks.
     * @return the trust manager.
     * @throws TrustManagerException if something goes wrong.
     */
    static X509TrustManager createTrustManager(String trustStoreLocation, char[] trustStorePassword,
                                               String trustStoreType, boolean crlEnabled, boolean ocspEnabled) throws TrustManagerException {

        if (trustStoreType == null) {
            trustStoreType = "jks";
        }

        if (trustStorePassword == null) {
            trustStorePassword = EMPTY_CHAR_ARRAY;
        }

        try {
            KeyStore ts = KeyStore.getInstance(trustStoreType);
            try (InputStream inputStream = Files.newInputStream(new File(trustStoreLocation).toPath())) {
                ts.load(inputStream, trustStorePassword);
            }

            PKIXBuilderParameters pbParams = new PKIXBuilderParameters(ts, new X509CertSelector());
            if (crlEnabled || ocspEnabled) {
                pbParams.setRevocationEnabled(true);
                System.setProperty("com.sun.net.ssl.checkRevocation", "true");
                if (crlEnabled) {
                    System.setProperty("com.sun.security.enableCRLDP", "true");
                }
                if (ocspEnabled) {
                    Security.setProperty("ocsp.enable", "true");
                }
            } else {
                pbParams.setRevocationEnabled(false);
            }

            // Revocation checking is only supported with the PKIX algorithm
            TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
            tmf.init(new CertPathTrustManagerParameters(pbParams));

            for (final TrustManager tm : tmf.getTrustManagers()) {
                if (tm instanceof X509ExtendedTrustManager) {
                    return (X509ExtendedTrustManager) tm;
                }
            }
            throw new TrustManagerException("Couldn't find X509TrustManager");
        } catch (IOException | GeneralSecurityException | IllegalArgumentException e) {
            throw new TrustManagerException(e);
        }
    }

    private static String[] getEnabledProtocols(String enabledProtocolsInput, String tlsConfigProtocols) {
        if (enabledProtocolsInput == null) {
            return tlsConfigProtocols.split(",");
        }
        return enabledProtocolsInput.split(",");
    }

}
