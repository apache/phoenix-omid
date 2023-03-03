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

import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.omid.TestUtils;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.tls.X509KeyType;
import org.apache.omid.tls.X509TestContext;
import org.apache.omid.tso.HALeaseManagementModule;
import org.apache.omid.tso.TSOMockModule;
import org.apache.omid.tso.TSOServer;
import org.apache.omid.tso.TSOServerConfig;
import org.apache.omid.tso.VoidLeaseManagementModule;
import org.apache.omid.tso.TSOServerConfig.TIMESTAMP_TYPE;
import org.apache.statemachine.StateMachine.FsmImpl;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.omid.tls.KeyStoreFileType;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.security.Security;
import java.util.concurrent.ExecutionException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTSOClientConnectionToTSO {

    private static final Logger LOG = LoggerFactory.getLogger(TestTSOClientConnectionToTSO.class);

    // Constants and variables for component connectivity
    private static final String TSO_HOST = "localhost";
    private static final String CURRENT_TSO_PATH = "/current_tso_path";
    private static final String TSO_LEASE_PATH = "/tso_lease_path";

    private int tsoPortForTest;
    private String zkClusterForTest;

    private Injector injector = null;

    private TestingServer zkServer;

    private CuratorFramework zkClient;
    private TSOServer tsoServer;

    protected X509TestContext x509TestContext;

    @BeforeMethod
    public void beforeMethod() throws Exception {

        tsoPortForTest = TestUtils.getFreeLocalPort();

        int zkPortForTest = TestUtils.getFreeLocalPort();
        zkClusterForTest = TSO_HOST + ":" + zkPortForTest;
        LOG.info("Starting ZK Server in port {}", zkPortForTest);
        zkServer = TestUtils.provideTestingZKServer(zkPortForTest);
        LOG.info("ZK Server Started @ {}", zkServer.getConnectString());

        zkClient = TestUtils.provideConnectedZKClient(zkClusterForTest);

        Stat stat;
        try {
            zkClient.delete().forPath(CURRENT_TSO_PATH);
            stat = zkClient.checkExists().forPath(CURRENT_TSO_PATH);
            assertNull(stat, CURRENT_TSO_PATH + " should not exist");
        } catch (NoNodeException e) {
            LOG.info("{} ZNode did not exist", CURRENT_TSO_PATH);
        }

    }

    @AfterMethod
    public void afterMethod() {

        zkClient.close();

        CloseableUtils.closeQuietly(zkServer);
        zkServer = null;
        LOG.info("ZK Server Stopped");

    }

    @Test(timeOut = 30_000)
    public void testUnsuccessfulConnectionToTSO() throws Exception {

        // When no HA node for TSOServer is found & no host:port config exists
        // we should get an exception when getting the client
        try {
            TSOClient.newInstance(new OmidClientConfiguration());
        } catch (IllegalArgumentException e) {
            // Expected
        }

    }

    @Test(timeOut = 30_000)
    public void testSuccessfulConnectionToTSOWithHostAndPort() throws Exception {

        // Launch a TSO WITHOUT publishing the address in HA...
        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setConflictMapSize(1000);
        tsoConfig.setPort(tsoPortForTest);
        tsoConfig.setTimestampType(TIMESTAMP_TYPE.INCREMENTAL.toString());
        tsoConfig.setLeaseModule(new VoidLeaseManagementModule());
        injector = Guice.createInjector(new TSOMockModule(tsoConfig));
        LOG.info("Starting TSO");
        tsoServer = injector.getInstance(TSOServer.class);
        tsoServer.startAsync();
        tsoServer.awaitRunning();
        TestUtils.waitForSocketListening(TSO_HOST, tsoPortForTest, 100);
        LOG.info("Finished loading TSO");

        // When no HA node for TSOServer is found we should get a connection
        // to the TSO through the host:port configured...
        OmidClientConfiguration tsoClientConf = new OmidClientConfiguration();
        tsoClientConf.setConnectionString("localhost:" + tsoPortForTest);
        tsoClientConf.setZkCurrentTsoPath(CURRENT_TSO_PATH);
        TSOClient tsoClient = TSOClient.newInstance(tsoClientConf);

        // ... so we should get responses from the methods
        Long startTS = tsoClient.getNewStartTimestamp().get();
        LOG.info("Start TS {} ", startTS);
        assertEquals(startTS.longValue(), CommitTable.MAX_CHECKPOINTS_PER_TXN);

        // Close the tsoClient connection and stop the TSO Server
        tsoClient.close().get();
        tsoServer.stopAsync();
        tsoServer.awaitTerminated();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_HOST, tsoPortForTest, 1000);
        LOG.info("TSO Server Stopped");

    }

    @Test(timeOut = 30_000)
    public void testSuccessfulConnectionToTSOWithHostAndPortSSLServerside() throws Exception {

        // Launch a TSO WITHOUT publishing the address in HA...
        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setConflictMapSize(1000);
        tsoConfig.setPort(tsoPortForTest);
        tsoConfig.setTimestampType(TIMESTAMP_TYPE.INCREMENTAL.toString());
        tsoConfig.setLeaseModule(new VoidLeaseManagementModule());
        tsoConfig.setTlsEnabled(true);
        tsoConfig.setSupportPlainText(true);

        Security.addProvider(new BouncyCastleProvider());
        File tempDir = Files.createTempDirectory("x509Tests").toFile();
        String keyPassword = "pa$$w0rd";
        X509KeyType certKeyType = X509KeyType.RSA;
        X509KeyType caKeyType = X509KeyType.RSA;

        x509TestContext = X509TestContext.newBuilder().setTempDir(tempDir).setKeyStorePassword(keyPassword)
                .setKeyStoreKeyType(certKeyType).setTrustStorePassword(keyPassword)
                .setTrustStoreKeyType(caKeyType).build();

        x509TestContext.setSystemProperties(KeyStoreFileType.JKS, KeyStoreFileType.JKS);

        LOG.info("trustStoreLocation :{}", x509TestContext.getTlsConfigKeystoreLocation());


        tsoConfig.setKeyStoreLocation(x509TestContext.getTlsConfigKeystoreLocation());;
        tsoConfig.setKeyStorePassword(x509TestContext.getTlsConfigKeystorePassword());
        tsoConfig.setKeyStoreType(x509TestContext.getTlsConfigKeystoreType());
        tsoConfig.setTrustStoreLocation(x509TestContext.getTlsConfigTrustLocation());
        tsoConfig.setTrustStorePassword(x509TestContext.getTlsConfigTrustPassword());
        tsoConfig.setTrustStoreType(x509TestContext.getTlsConfigTrustType());

        injector = Guice.createInjector(new TSOMockModule(tsoConfig));
        LOG.info("Starting TSO");
        tsoServer = injector.getInstance(TSOServer.class);
        tsoServer.startAsync();
        tsoServer.awaitRunning();
        TestUtils.waitForSocketListening(TSO_HOST, tsoPortForTest, 300);
        LOG.info("Finished loading TSO");

        // When no HA node for TSOServer is found we should get a connection
        // to the TSO through the host:port configured...
        OmidClientConfiguration tsoClientConf = new OmidClientConfiguration();
        tsoClientConf.setConnectionString("localhost:" + tsoPortForTest);
        tsoClientConf.setZkCurrentTsoPath(CURRENT_TSO_PATH);
        tsoClientConf.setTlsEnabled(false);
        tsoClientConf.setKeyStoreLocation(x509TestContext.getTlsConfigKeystoreLocation());;
        tsoClientConf.setKeyStorePassword(x509TestContext.getTlsConfigKeystorePassword());
        tsoClientConf.setKeyStoreType(x509TestContext.getTlsConfigKeystoreType());
        tsoClientConf.setTrustStoreLocation(x509TestContext.getTlsConfigTrustLocation());
        tsoClientConf.setTrustStorePassword(x509TestContext.getTlsConfigTrustPassword());
        tsoClientConf.setTrustStoreType(x509TestContext.getTlsConfigTrustType());
        TSOClient tsoClient = TSOClient.newInstance(tsoClientConf);

        // ... so we should get responses from the methods
        Long startTS = tsoClient.getNewStartTimestamp().get();
        LOG.info("Start TS {} ", startTS);
        assertEquals(startTS.longValue(), CommitTable.MAX_CHECKPOINTS_PER_TXN);

        // Close the tsoClient connection and stop the TSO Server
        tsoClient.close().get();
        tsoServer.stopAsync();
        tsoServer.awaitTerminated();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_HOST, tsoPortForTest, 1000);
        LOG.info("TSO Server Stopped");
    }


    @Test(timeOut = 30_000)
    public void testSuccessfulConnectionToTSOWithHostAndPortSSLBoth() throws Exception {

        // Launch a TSO WITHOUT publishing the address in HA...
        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setConflictMapSize(1000);
        tsoConfig.setPort(tsoPortForTest);
        tsoConfig.setTimestampType(TIMESTAMP_TYPE.INCREMENTAL.toString());
        tsoConfig.setLeaseModule(new VoidLeaseManagementModule());
        tsoConfig.setTlsEnabled(true);
        tsoConfig.setSupportPlainText(false);

        Security.addProvider(new BouncyCastleProvider());
        File tempDir = Files.createTempDirectory("x509Tests").toFile();
        String keyPassword = "pa$$w0rd";
        X509KeyType certKeyType = X509KeyType.RSA;
        X509KeyType caKeyType = X509KeyType.RSA;

        x509TestContext = X509TestContext.newBuilder().setTempDir(tempDir).setKeyStorePassword(keyPassword)
                .setKeyStoreKeyType(certKeyType).setTrustStorePassword(keyPassword)
                .setTrustStoreKeyType(caKeyType).build();

        x509TestContext.setSystemProperties(KeyStoreFileType.JKS, KeyStoreFileType.JKS);

        LOG.info("trustStoreLocation :{}", x509TestContext.getTlsConfigKeystoreLocation());


        tsoConfig.setKeyStoreLocation(x509TestContext.getTlsConfigKeystoreLocation());;
        tsoConfig.setKeyStorePassword(x509TestContext.getTlsConfigKeystorePassword());
        tsoConfig.setKeyStoreType(x509TestContext.getTlsConfigKeystoreType());
        tsoConfig.setSslCrlEnabled(true);

        injector = Guice.createInjector(new TSOMockModule(tsoConfig));
        LOG.info("Starting TSO");
        tsoServer = injector.getInstance(TSOServer.class);
        tsoServer.startAsync();
        tsoServer.awaitRunning();
        TestUtils.waitForSocketListening(TSO_HOST, tsoPortForTest, 300);
        LOG.info("Finished loading TSO");

        // When no HA node for TSOServer is found we should get a connection
        // to the TSO through the host:port configured...
        OmidClientConfiguration tsoClientConf = new OmidClientConfiguration();
        tsoClientConf.setConnectionString("localhost:" + tsoPortForTest);
        tsoClientConf.setZkCurrentTsoPath(CURRENT_TSO_PATH);
        tsoClientConf.setTlsEnabled(true);
        tsoClientConf.setTrustStoreLocation(x509TestContext.getTlsConfigTrustLocation());
        tsoClientConf.setTrustStorePassword(x509TestContext.getTlsConfigTrustPassword());
        tsoClientConf.setTrustStoreType(x509TestContext.getTlsConfigTrustType());

        TSOClient tsoClient = TSOClient.newInstance(tsoClientConf);

        // ... so we should get responses from the methods
        Long startTS = tsoClient.getNewStartTimestamp().get();
        LOG.info("Start TS {} ", startTS);
        assertEquals(startTS.longValue(), CommitTable.MAX_CHECKPOINTS_PER_TXN);

        // Close the tsoClient connection and stop the TSO Server
        tsoClient.close().get();
        tsoServer.stopAsync();
        tsoServer.awaitTerminated();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_HOST, tsoPortForTest, 1000);
        LOG.info("TSO Server Stopped");

    }

    @Test(timeOut = 30_000)
    public void testSuccessfulConnectionToTSOWithHostAndPortBCFKS() throws Exception {

        // Launch a TSO WITHOUT publishing the address in HA...
        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setConflictMapSize(1000);
        tsoConfig.setPort(tsoPortForTest);
        tsoConfig.setTimestampType(TIMESTAMP_TYPE.INCREMENTAL.toString());
        tsoConfig.setLeaseModule(new VoidLeaseManagementModule());
        tsoConfig.setTlsEnabled(true);
        tsoConfig.setSupportPlainText(false);

        Security.addProvider(new BouncyCastleProvider());
        File tempDir = Files.createTempDirectory("x509Tests").toFile();
        String keyPassword = "pa$$w0rd";
        X509KeyType certKeyType = X509KeyType.RSA;
        X509KeyType caKeyType = X509KeyType.RSA;

        x509TestContext = X509TestContext.newBuilder().setTempDir(tempDir).setKeyStorePassword(keyPassword)
                .setKeyStoreKeyType(certKeyType).setTrustStorePassword(keyPassword)
                .setTrustStoreKeyType(caKeyType).build();

        x509TestContext.setSystemProperties(KeyStoreFileType.BCFKS, KeyStoreFileType.BCFKS);

        LOG.info("trustStoreLocation :{}", x509TestContext.getTlsConfigKeystoreLocation());


        tsoConfig.setKeyStoreLocation(x509TestContext.getTlsConfigKeystoreLocation());;
        tsoConfig.setKeyStorePassword(x509TestContext.getTlsConfigKeystorePassword());
        tsoConfig.setKeyStoreType(x509TestContext.getTlsConfigKeystoreType());
        tsoConfig.setSslCrlEnabled(true);

        injector = Guice.createInjector(new TSOMockModule(tsoConfig));
        LOG.info("Starting TSO");
        tsoServer = injector.getInstance(TSOServer.class);
        tsoServer.startAsync();
        tsoServer.awaitRunning();
        TestUtils.waitForSocketListening(TSO_HOST, tsoPortForTest, 300);
        LOG.info("Finished loading TSO");

        // When no HA node for TSOServer is found we should get a connection
        // to the TSO through the host:port configured...
        OmidClientConfiguration tsoClientConf = new OmidClientConfiguration();
        tsoClientConf.setConnectionString("localhost:" + tsoPortForTest);
        tsoClientConf.setZkCurrentTsoPath(CURRENT_TSO_PATH);
        tsoClientConf.setTlsEnabled(true);
        tsoClientConf.setTrustStoreLocation(x509TestContext.getTlsConfigTrustLocation());
        tsoClientConf.setTrustStorePassword(x509TestContext.getTlsConfigTrustPassword());
        tsoClientConf.setTrustStoreType(x509TestContext.getTlsConfigTrustType());

        TSOClient tsoClient = TSOClient.newInstance(tsoClientConf);

        // ... so we should get responses from the methods
        Long startTS = tsoClient.getNewStartTimestamp().get();
        LOG.info("Start TS {} ", startTS);
        assertEquals(startTS.longValue(), CommitTable.MAX_CHECKPOINTS_PER_TXN);

        // Close the tsoClient connection and stop the TSO Server
        tsoClient.close().get();
        tsoServer.stopAsync();
        tsoServer.awaitTerminated();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_HOST, tsoPortForTest, 1000);
        LOG.info("TSO Server Stopped");

    }

    @Test(timeOut = 30_000)
    public void testSuccessfulConnectionToTSOWithHostAndPortSSLBothMutual() throws Exception {

        // Launch a TSO WITHOUT publishing the address in HA...
        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setConflictMapSize(1000);
        tsoConfig.setPort(tsoPortForTest);
        tsoConfig.setTimestampType(TIMESTAMP_TYPE.INCREMENTAL.toString());
        tsoConfig.setLeaseModule(new VoidLeaseManagementModule());
        tsoConfig.setTlsEnabled(true);
        tsoConfig.setSupportPlainText(false);

        Security.addProvider(new BouncyCastleProvider());
        File tempDir = Files.createTempDirectory("x509Tests").toFile();
        String keyPassword = "pa$$w0rd";
        X509KeyType certKeyType = X509KeyType.RSA;
        X509KeyType caKeyType = X509KeyType.RSA;

        x509TestContext = X509TestContext.newBuilder().setTempDir(tempDir).setKeyStorePassword(keyPassword)
                .setKeyStoreKeyType(certKeyType).setTrustStorePassword(keyPassword)
                .setTrustStoreKeyType(caKeyType).build();

        x509TestContext.setSystemProperties(KeyStoreFileType.JKS, KeyStoreFileType.JKS);

        LOG.info("trustStoreLocation :{}", x509TestContext.getTlsConfigKeystoreLocation());


        tsoConfig.setKeyStoreLocation(x509TestContext.getTlsConfigKeystoreLocation());;
        tsoConfig.setKeyStorePassword(x509TestContext.getTlsConfigKeystorePassword());
        tsoConfig.setKeyStoreType(x509TestContext.getTlsConfigKeystoreType());
        tsoConfig.setTrustStoreLocation(x509TestContext.getTlsConfigTrustLocation());
        tsoConfig.setTrustStorePassword(x509TestContext.getTlsConfigTrustPassword());
        tsoConfig.setTrustStoreType(x509TestContext.getTlsConfigTrustType());
        tsoConfig.setSslCrlEnabled(true);


        injector = Guice.createInjector(new TSOMockModule(tsoConfig));
        LOG.info("Starting TSO");
        tsoServer = injector.getInstance(TSOServer.class);
        tsoServer.startAsync();
        tsoServer.awaitRunning();
        TestUtils.waitForSocketListening(TSO_HOST, tsoPortForTest, 300);
        LOG.info("Finished loading TSO");

        // When no HA node for TSOServer is found we should get a connection
        // to the TSO through the host:port configured...
        OmidClientConfiguration tsoClientConf = new OmidClientConfiguration();
        tsoClientConf.setConnectionString("localhost:" + tsoPortForTest);
        tsoClientConf.setZkCurrentTsoPath(CURRENT_TSO_PATH);
        tsoClientConf.setTlsEnabled(true);
        tsoClientConf.setKeyStoreLocation(x509TestContext.getTlsConfigKeystoreLocation());;
        tsoClientConf.setKeyStorePassword(x509TestContext.getTlsConfigKeystorePassword());
        tsoClientConf.setKeyStoreType(x509TestContext.getTlsConfigKeystoreType());
        tsoClientConf.setTrustStoreLocation(x509TestContext.getTlsConfigTrustLocation());
        tsoClientConf.setTrustStorePassword(x509TestContext.getTlsConfigTrustPassword());
        tsoClientConf.setTrustStoreType(x509TestContext.getTlsConfigTrustType());

        TSOClient tsoClient = TSOClient.newInstance(tsoClientConf);

        // ... so we should get responses from the methods
        Long startTS = tsoClient.getNewStartTimestamp().get();
        LOG.info("Start TS {} ", startTS);
        assertEquals(startTS.longValue(), CommitTable.MAX_CHECKPOINTS_PER_TXN);

        // Close the tsoClient connection and stop the TSO Server
        tsoClient.close().get();
        tsoServer.stopAsync();
        tsoServer.awaitTerminated();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_HOST, tsoPortForTest, 1000);
        LOG.info("TSO Server Stopped");

    }

    @Test(timeOut = 30_000)
    public void testSuccessfulConnectionToTSOWithHostAndPortSSLCipherSuite() throws Exception {

        // Launch a TSO WITHOUT publishing the address in HA...
        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setConflictMapSize(1000);
        tsoConfig.setPort(tsoPortForTest);
        tsoConfig.setTimestampType(TIMESTAMP_TYPE.INCREMENTAL.toString());
        tsoConfig.setLeaseModule(new VoidLeaseManagementModule());
        tsoConfig.setTlsEnabled(true);
        tsoConfig.setSupportPlainText(false);

        Security.addProvider(new BouncyCastleProvider());
        File tempDir = Files.createTempDirectory("x509Tests").toFile();
        String keyPassword = "pa$$w0rd";
        X509KeyType certKeyType = X509KeyType.RSA;
        X509KeyType caKeyType = X509KeyType.RSA;

        x509TestContext = X509TestContext.newBuilder().setTempDir(tempDir).setKeyStorePassword(keyPassword)
                .setKeyStoreKeyType(certKeyType).setTrustStorePassword(keyPassword)
                .setTrustStoreKeyType(caKeyType).build();

        x509TestContext.setSystemProperties(KeyStoreFileType.JKS, KeyStoreFileType.JKS);

        LOG.info("trustStoreLocation :{}", x509TestContext.getTlsConfigKeystoreLocation());


        tsoConfig.setKeyStoreLocation(x509TestContext.getTlsConfigKeystoreLocation());;
        tsoConfig.setKeyStorePassword(x509TestContext.getTlsConfigKeystorePassword());
        tsoConfig.setKeyStoreType(x509TestContext.getTlsConfigKeystoreType());

        tsoConfig.setCipherSuites("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384");


        injector = Guice.createInjector(new TSOMockModule(tsoConfig));
        LOG.info("Starting TSO");
        tsoServer = injector.getInstance(TSOServer.class);
        tsoServer.startAsync();
        tsoServer.awaitRunning();
        TestUtils.waitForSocketListening(TSO_HOST, tsoPortForTest, 300);
        LOG.info("Finished loading TSO");

        // When no HA node for TSOServer is found we should get a connection
        // to the TSO through the host:port configured...
        OmidClientConfiguration tsoClientConf = new OmidClientConfiguration();
        tsoClientConf.setConnectionString("localhost:" + tsoPortForTest);
        tsoClientConf.setZkCurrentTsoPath(CURRENT_TSO_PATH);
        tsoClientConf.setTlsEnabled(true);
        tsoClientConf.setTrustStoreLocation(x509TestContext.getTlsConfigTrustLocation());
        tsoClientConf.setTrustStorePassword(x509TestContext.getTlsConfigTrustPassword());
        tsoClientConf.setTrustStoreType(x509TestContext.getTlsConfigTrustType());
        tsoClientConf.setCipherSuites("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384");

        TSOClient tsoClient = TSOClient.newInstance(tsoClientConf);

        // ... so we should get responses from the methods
        Long startTS = tsoClient.getNewStartTimestamp().get();
        LOG.info("Start TS {} ", startTS);
        assertEquals(startTS.longValue(), CommitTable.MAX_CHECKPOINTS_PER_TXN);

        // Close the tsoClient connection and stop the TSO Server
        tsoClient.close().get();
        tsoServer.stopAsync();
        tsoServer.awaitTerminated();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_HOST, tsoPortForTest, 1000);
        LOG.info("TSO Server Stopped");

    }

    @Test(timeOut = 30_000)
    public void testSuccessfulConnectionToTSOWithHostAndPortSSLOneCipherSuite2() throws Exception {

        // Launch a TSO WITHOUT publishing the address in HA...
        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setConflictMapSize(1000);
        tsoConfig.setPort(tsoPortForTest);
        tsoConfig.setTimestampType(TIMESTAMP_TYPE.INCREMENTAL.toString());
        tsoConfig.setLeaseModule(new VoidLeaseManagementModule());
        tsoConfig.setTlsEnabled(true);
        tsoConfig.setSupportPlainText(false);

        Security.addProvider(new BouncyCastleProvider());
        File tempDir = Files.createTempDirectory("x509Tests").toFile();
        String keyPassword = "pa$$w0rd";
        X509KeyType certKeyType = X509KeyType.RSA;
        X509KeyType caKeyType = X509KeyType.RSA;

        x509TestContext = X509TestContext.newBuilder().setTempDir(tempDir).setKeyStorePassword(keyPassword)
                .setKeyStoreKeyType(certKeyType).setTrustStorePassword(keyPassword)
                .setTrustStoreKeyType(caKeyType).build();

        x509TestContext.setSystemProperties(KeyStoreFileType.JKS, KeyStoreFileType.JKS);

        LOG.info("trustStoreLocation :{}", x509TestContext.getTlsConfigKeystoreLocation());


        tsoConfig.setKeyStoreLocation(x509TestContext.getTlsConfigKeystoreLocation());;
        tsoConfig.setKeyStorePassword(x509TestContext.getTlsConfigKeystorePassword());
        tsoConfig.setKeyStoreType(x509TestContext.getTlsConfigKeystoreType());

        tsoConfig.setCipherSuites("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA");


        injector = Guice.createInjector(new TSOMockModule(tsoConfig));
        LOG.info("Starting TSO");
        tsoServer = injector.getInstance(TSOServer.class);
        tsoServer.startAsync();
        tsoServer.awaitRunning();
        TestUtils.waitForSocketListening(TSO_HOST, tsoPortForTest, 300);
        LOG.info("Finished loading TSO");

        // When no HA node for TSOServer is found we should get a connection
        // to the TSO through the host:port configured...
        OmidClientConfiguration tsoClientConf = new OmidClientConfiguration();
        tsoClientConf.setConnectionString("localhost:" + tsoPortForTest);
        tsoClientConf.setZkCurrentTsoPath(CURRENT_TSO_PATH);
        tsoClientConf.setTlsEnabled(true);
        tsoClientConf.setTrustStoreLocation(x509TestContext.getTlsConfigTrustLocation());
        tsoClientConf.setTrustStorePassword(x509TestContext.getTlsConfigTrustPassword());
        tsoClientConf.setTrustStoreType(x509TestContext.getTlsConfigTrustType());
        tsoClientConf.setCipherSuites("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA");

        TSOClient tsoClient = TSOClient.newInstance(tsoClientConf);

        // ... so we should get responses from the methods
        Long startTS = tsoClient.getNewStartTimestamp().get();
        LOG.info("Start TS {} ", startTS);
        assertEquals(startTS.longValue(), CommitTable.MAX_CHECKPOINTS_PER_TXN);

        // Close the tsoClient connection and stop the TSO Server
        tsoClient.close().get();
        tsoServer.stopAsync();
        tsoServer.awaitTerminated();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_HOST, tsoPortForTest, 1000);
        LOG.info("TSO Server Stopped");

    }

    @Test(timeOut = 30_000)
    public void testSuccessfulConnectionToTSOThroughZK() throws Exception {

        // Launch a TSO publishing the address in HA...
        TSOServerConfig config = new TSOServerConfig();
        config.setConflictMapSize(1000);
        config.setPort(tsoPortForTest);
        config.setLeaseModule(new HALeaseManagementModule(1000, TSO_LEASE_PATH, CURRENT_TSO_PATH, zkClusterForTest, "omid"));
        injector = Guice.createInjector(new TSOMockModule(config));
        LOG.info("Starting TSO");
        tsoServer = injector.getInstance(TSOServer.class);
        tsoServer.startAsync();
        tsoServer.awaitRunning();
        TestUtils.waitForSocketListening(TSO_HOST, tsoPortForTest, 100);
        LOG.info("Finished loading TSO");

        waitTillTsoRegisters(injector.getInstance(CuratorFramework.class));

        // When a HA node for TSOServer is found we should get a connection
        OmidClientConfiguration tsoClientConf = new OmidClientConfiguration();
        tsoClientConf.setConnectionType(OmidClientConfiguration.ConnType.HA);
        tsoClientConf.setConnectionString(zkClusterForTest);
        tsoClientConf.setZkCurrentTsoPath(CURRENT_TSO_PATH);
        TSOClient tsoClient = TSOClient.newInstance(tsoClientConf);

        // ... so we should get responses from the methods
        Long startTS = tsoClient.getNewStartTimestamp().get();
        LOG.info("Start TS {} ", startTS);
        assertTrue(startTS.longValue() >= CommitTable.MAX_CHECKPOINTS_PER_TXN);

        // Close the tsoClient connection and stop the TSO Server
        tsoClient.close().get();
        tsoServer.stopAsync();
        tsoServer.awaitTerminated();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_HOST, tsoPortForTest, 1000);
        LOG.info("TSO Server Stopped");

    }

    @Test(timeOut = 30_000)
    public void testSuccessOfTSOClientReconnectionsToARestartedTSOWithZKPublishing() throws Exception {

        // Start a TSO with HA...
        TSOServerConfig config = new TSOServerConfig();
        config.setConflictMapSize(1000);
        config.setPort(tsoPortForTest);
        config.setLeaseModule(new HALeaseManagementModule(1000, TSO_LEASE_PATH, CURRENT_TSO_PATH, zkClusterForTest, "omid"));
        injector = Guice.createInjector(new TSOMockModule(config));
        LOG.info("Starting Initial TSO");
        tsoServer = injector.getInstance(TSOServer.class);
        tsoServer.startAsync();
        tsoServer.awaitRunning();
        TestUtils.waitForSocketListening(TSO_HOST, tsoPortForTest, 100);
        LOG.info("Finished loading TSO");

        waitTillTsoRegisters(injector.getInstance(CuratorFramework.class));

        // Then create the TSO Client under test...
        OmidClientConfiguration tsoClientConf = new OmidClientConfiguration();
        tsoClientConf.setConnectionType(OmidClientConfiguration.ConnType.HA);
        tsoClientConf.setConnectionString(zkClusterForTest);
        tsoClientConf.setZkCurrentTsoPath(CURRENT_TSO_PATH);
        TSOClient tsoClient = TSOClient.newInstance(tsoClientConf);

        // ... and check that initially we get responses from the methods
        Long startTS = tsoClient.getNewStartTimestamp().get();
        LOG.info("Start TS {} ", startTS);
        assertTrue(startTS.longValue() >= CommitTable.MAX_CHECKPOINTS_PER_TXN);

        // Then stop the server...
        tsoServer.stopAsync();
        tsoServer.awaitTerminated();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_HOST, tsoPortForTest, 1000);
        LOG.info("Initial TSO Server Stopped");

        Thread.sleep(1500); // ...allow the client to receive disconnection event...
        // ... and check that we get a conn exception when trying to access the client
        try {
            startTS = tsoClient.getNewStartTimestamp().get();
            fail();
        } catch (ExecutionException e) {
            LOG.info("Exception expected");
            // Internal accessor to fsm to do the required checkings
            FsmImpl fsm = (FsmImpl) tsoClient.fsm;
            assertEquals(e.getCause().getClass(), ConnectionException.class);
            assertTrue(fsm.getState().getClass().equals(TSOClient.ConnectionFailedState.class)
                               ||
                               fsm.getState().getClass().equals(TSOClient.DisconnectedState.class));
        }

        // After that, simulate that a new TSO has been launched...
        Injector newInjector = Guice.createInjector(new TSOMockModule(config));
        LOG.info("Re-Starting again the TSO");
        tsoServer = newInjector.getInstance(TSOServer.class);
        tsoServer.startAsync();
        tsoServer.awaitRunning();
        TestUtils.waitForSocketListening(TSO_HOST, tsoPortForTest, 100);
        LOG.info("Finished loading restarted TSO");

        // Finally re-check that, eventually, we can get a new value from the new TSO...
        boolean reconnectionActive = false;
        while (!reconnectionActive) {
            try {
                startTS = tsoClient.getNewStartTimestamp().get();
                reconnectionActive = true;
            } catch (ExecutionException e) {
                // Expected
            }
        }
        assertNotNull(startTS);

        // ...and stop the server
        tsoServer.stopAsync();
        tsoServer.awaitTerminated();
        TestUtils.waitForSocketNotListening(TSO_HOST, tsoPortForTest, 1000);
        LOG.info("Restarted TSO Server Stopped");
    }

    private void waitTillTsoRegisters(CuratorFramework zkClient) throws Exception {
        while (true) {
            try {
                Stat stat = zkClient.checkExists().forPath(CURRENT_TSO_PATH);
                if (stat == null) {
                    continue;
                }
                LOG.info("TSO registered in HA with path {}={}", CURRENT_TSO_PATH, stat.toString());
                if (stat.toString().length() == 0) {
                    continue;
                }
                return;
            } catch (Exception e) {
                LOG.debug("TSO still has not registered yet, sleeping...", e);
                Thread.sleep(500);
            }
        }
    }

}
