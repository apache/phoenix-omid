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
package org.apache.omid.transaction;

import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.TestUtils;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.hbase.HBaseCommitTableConfig;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.timestamp.storage.HBaseTimestampStorageConfig;
import org.apache.omid.tso.TSOServer;
import org.apache.omid.tso.TSOServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestSnapshotFilterLL {

    private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotFilterLL.class);

    private static final String TEST_FAMILY = "test-fam";

    private static final int MAX_VERSIONS = 3;

    private AbstractTransactionManager tm;

    private Injector injector;

    private Admin admin;
    private Configuration hbaseConf;
    private HBaseTestingUtility hbaseTestUtil;
    private MiniHBaseCluster hbaseCluster;

    private TSOServer tso;

    private CommitTable commitTable;
    private PostCommitActions syncPostCommitter;
    private Connection connection;

    @BeforeClass
    public void setupTestSnapshotFilter() throws Exception {
        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setPort(5678);
        tsoConfig.setConflictMapSize(1);
        tsoConfig.setWaitStrategy("LOW_CPU");
        tsoConfig.setLowLatency(true);
        injector = Guice.createInjector(new TSOForSnapshotFilterTestModule(tsoConfig));
        hbaseConf = injector.getInstance(Configuration.class);
        hbaseConf.setBoolean("omid.server.side.filter", true);
        hbaseConf.setInt("hbase.hconnection.threads.core", 5);
        hbaseConf.setInt("hbase.hconnection.threads.max", 10);
        // Tunn down handler threads in regionserver
        hbaseConf.setInt("hbase.regionserver.handler.count", 10);

        // Set to random port
        hbaseConf.setInt("hbase.master.port", 0);
        hbaseConf.setInt("hbase.master.info.port", 0);
        hbaseConf.setInt("hbase.regionserver.port", 0);
        hbaseConf.setInt("hbase.regionserver.info.port", 0);


        HBaseCommitTableConfig hBaseCommitTableConfig = injector.getInstance(HBaseCommitTableConfig.class);
        HBaseTimestampStorageConfig hBaseTimestampStorageConfig = injector.getInstance(HBaseTimestampStorageConfig.class);

        setupHBase();
        connection = ConnectionFactory.createConnection(hbaseConf);
        admin = connection.getAdmin();
        createRequiredHBaseTables(hBaseTimestampStorageConfig, hBaseCommitTableConfig);
        setupTSO();

        commitTable = injector.getInstance(CommitTable.class);
    }

    private void setupHBase() throws Exception {
        LOG.info("--------------------------------------------------------------------------------------------------");
        LOG.info("Setting up HBase");
        LOG.info("--------------------------------------------------------------------------------------------------");
        hbaseTestUtil = new HBaseTestingUtility(hbaseConf);
        LOG.info("--------------------------------------------------------------------------------------------------");
        LOG.info("Creating HBase MiniCluster");
        LOG.info("--------------------------------------------------------------------------------------------------");
        hbaseCluster = hbaseTestUtil.startMiniCluster(1);
    }

    private void createRequiredHBaseTables(HBaseTimestampStorageConfig timestampStorageConfig,
                                           HBaseCommitTableConfig hBaseCommitTableConfig) throws IOException {
        createTableIfNotExists(timestampStorageConfig.getTableName(), timestampStorageConfig.getFamilyName().getBytes());

        createTableIfNotExists(hBaseCommitTableConfig.getTableName(), hBaseCommitTableConfig.getCommitTableFamily(), hBaseCommitTableConfig.getLowWatermarkFamily());
    }

    private void createTableIfNotExists(String tableName, byte[]... families) throws IOException {
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            LOG.info("Creating {} table...", tableName);
            
            ArrayList<ColumnFamilyDescriptor> fams = new ArrayList<>();
            for (byte[] family : families) {
                fams.add(ColumnFamilyDescriptorBuilder
                    .newBuilder(family)
                    .setMaxVersions(MAX_VERSIONS)
                    .build());
            }
            int priority = Coprocessor.PRIORITY_HIGHEST;
            TableDescriptor desc = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf(tableName))
                    .setColumnFamilies(fams)
                    .setCoprocessor(CoprocessorDescriptorBuilder
                            .newBuilder(OmidSnapshotFilter.class.getName())
                            .setPriority(++priority).build())
                    .setCoprocessor(CoprocessorDescriptorBuilder
                            .newBuilder("org.apache.hadoop.hbase.coprocessor.AggregateImplementation")
                            .setPriority(++priority).build())
                    .build();
            admin.createTable(desc);
            try {
                hbaseTestUtil.waitTableAvailable(TableName.valueOf(tableName),5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private void setupTSO() throws IOException, InterruptedException {
        tso = injector.getInstance(TSOServer.class);
        tso.startAsync();
        tso.awaitRunning();
        TestUtils.waitForSocketListening("localhost", 5678, 100);
        Thread.currentThread().setName("UnitTest(s) thread");
    }

    @AfterClass
    public void cleanupTestSnapshotFilter() throws Exception {
        teardownTSO();
        hbaseCluster.shutdown();
    }

    private void teardownTSO() throws IOException, InterruptedException {
        tso.stopAsync();
        tso.awaitTerminated();
        TestUtils.waitForSocketNotListening("localhost", 5678, 1000);
    }

    @BeforeMethod
    public void setupTestSnapshotFilterIndividualTest() throws Exception {
        tm = spy((AbstractTransactionManager) newTransactionManager());
    }

    private TransactionManager newTransactionManager() throws Exception {
        HBaseOmidClientConfiguration hbaseOmidClientConf = new HBaseOmidClientConfiguration();
        hbaseOmidClientConf.setConnectionString("localhost:5678");
        hbaseOmidClientConf.setHBaseConfiguration(hbaseConf);
        CommitTable.Client commitTableClient = commitTable.getClient();
        syncPostCommitter =
                spy(new HBaseSyncPostCommitter(new NullMetricsProvider(),commitTableClient, connection));
        return HBaseTransactionManager.builder(hbaseOmidClientConf)
                .postCommitter(syncPostCommitter)
                .commitTableClient(commitTableClient)
                .build();
    }


    @Test(timeOut = 60_000)
    public void testInvalidate() throws Throwable {
        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");

        String TEST_TABLE = "testGetFirstResult";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction tx1 = tm.begin();

        Put row1 = new Put(rowName1);
        row1.addColumn(famName1, colName1, dataValue1);
        tt.put(tx1, row1);


        Transaction tx2 = tm.begin();

        Get get = new Get(rowName1);
        Result result = tt.get(tx2, get);

        assertTrue(result.isEmpty(), "Result should not be empty!");


        boolean gotInvalidated = false;
        try {
            tm.commit(tx1);
        } catch (RollbackException e) {
            gotInvalidated = true;
        }
        assertTrue(gotInvalidated);
        assertTrue(tm.isLowLatency());
    }

    @Test(timeOut = 60_000)
    public void testInvalidateByScan() throws Throwable {
        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");

        String TEST_TABLE = "testGetFirstResult";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction tx1 = tm.begin();

        Put row1 = new Put(rowName1);
        row1.addColumn(famName1, colName1, dataValue1);
        tt.put(tx1, row1);


        Transaction tx2 = tm.begin();

        ResultScanner iterableRS = tt.getScanner(tx2, new Scan().withStartRow(rowName1).withStopRow(rowName1, true));
        assertTrue(iterableRS.next() == null);

        tm.commit(tx2);

        boolean gotInvalidated = false;
        try {
            tm.commit(tx1);
        } catch (RollbackException e) {
            gotInvalidated = true;
        }
        assertTrue(gotInvalidated);
        assertTrue(tm.isLowLatency());
    }

}
