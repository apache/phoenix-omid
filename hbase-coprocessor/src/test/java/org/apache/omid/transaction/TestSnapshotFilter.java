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

import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
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

import java.io.IOException;

import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSnapshotFilter {

    private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotFilter.class);

    private static final String TEST_FAMILY = "test-fam";
    
    private static final int MAX_VERSIONS = 3;

    private AbstractTransactionManager tm;

    private Injector injector;

    private HBaseAdmin admin;
    private Configuration hbaseConf;
    private HBaseTestingUtility hbaseTestUtil;
    private MiniHBaseCluster hbaseCluster;

    private TSOServer tso;

    private AggregationClient aggregationClient;
    private CommitTable commitTable;
    private PostCommitActions syncPostCommitter;

    @BeforeClass
    public void setupTestSnapshotFilter() throws Exception {
        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setPort(5678);
        tsoConfig.setConflictMapSize(1);
        injector = Guice.createInjector(new TSOForSnapshotFilterTestModule(tsoConfig));
        hbaseConf = injector.getInstance(Configuration.class);
        hbaseConf.setBoolean("omid.server.side.filter", true);
        hbaseConf.setInt("hbase.master.info.port", 16011);
        HBaseCommitTableConfig hBaseCommitTableConfig = injector.getInstance(HBaseCommitTableConfig.class);
        HBaseTimestampStorageConfig hBaseTimestampStorageConfig = injector.getInstance(HBaseTimestampStorageConfig.class);

        setupHBase();
        aggregationClient = new AggregationClient(hbaseConf);
        admin = new HBaseAdmin(hbaseConf);
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
        if (!admin.tableExists(tableName)) {
            LOG.info("Creating {} table...", tableName);
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));

            for (byte[] family : families) {
                HColumnDescriptor datafam = new HColumnDescriptor(family);
                datafam.setMaxVersions(MAX_VERSIONS);
                desc.addFamily(datafam);
            }

            desc.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation");
            admin.createTable(desc);
        }

    }

    private void setupTSO() throws IOException, InterruptedException {
        tso = injector.getInstance(TSOServer.class);
        tso.startAndWait();
        TestUtils.waitForSocketListening("localhost", 5678, 100);
        Thread.currentThread().setName("UnitTest(s) thread");
    }

    @AfterClass
    public void cleanupTestSnapshotFilter() throws Exception {
        teardownTSO();
        hbaseCluster.shutdown();
    }

    private void teardownTSO() throws IOException, InterruptedException {
        tso.stopAndWait();
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
                spy(new HBaseSyncPostCommitter(new NullMetricsProvider(),commitTableClient));
        return HBaseTransactionManager.builder(hbaseOmidClientConf)
                .postCommitter(syncPostCommitter)
                .commitTableClient(commitTableClient)
                .build();
    }

    @Test(timeOut = 60_000)
    public void testGetFirstResult() throws Throwable {

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");

        String TEST_TABLE = "testGetFirstResult";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Transaction tx1 = tm.begin();

        Put row1 = new Put(rowName1);
        row1.add(famName1, colName1, dataValue1);
        tt.put(tx1, row1);
     
        tm.commit(tx1);

        Transaction tx2 = tm.begin();

        Get get = new Get(rowName1);
        Result result = tt.get(tx2, get);

        assertTrue(!result.isEmpty(), "Result should not be empty!");

        long tsRow = result.rawCells()[0].getTimestamp();
        assertEquals(tsRow, tx1.getTransactionId(), "Reading differnt version");

        tm.commit(tx2);

        Transaction tx3 = tm.begin();

        Put put3 = new Put(rowName1);
        put3.add(famName1, colName1, dataValue1);
        tt.put(tx3, put3);

        tm.commit(tx3);
        
        Transaction tx4 = tm.begin();

        Get get2 = new Get(rowName1);
        Result result2 = tt.get(tx4, get2);

        assertTrue(!result2.isEmpty(), "Result should not be empty!");

        long tsRow2 = result2.rawCells()[0].getTimestamp();
        assertEquals(tsRow2, tx3.getTransactionId(), "Reading differnt version");

        tm.commit(tx4);

        tt.close();
    }

    @Test(timeOut = 60_000)
    public void testGetSecondResult() throws Throwable {

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");

        String TEST_TABLE = "testGetFirstResult";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Transaction tx1 = tm.begin();

        Put put1 = new Put(rowName1);
        put1.add(famName1, colName1, dataValue1);
        tt.put(tx1, put1);
        
        tm.commit(tx1);

        Transaction tx2 = tm.begin();
        Put put2 = new Put(rowName1);
        put2.add(famName1, colName1, dataValue1);
        tt.put(tx2, put2);
        
        Transaction tx3 = tm.begin();

        Get get = new Get(rowName1);
        Result result = tt.get(tx3, get);

        assertTrue(!result.isEmpty(), "Result should not be empty!");

        long tsRow = result.rawCells()[0].getTimestamp();
        assertEquals(tsRow, tx1.getTransactionId(), "Reading differnt version");

        tm.commit(tx3);

        tt.close();
    }

    @Test(timeOut = 60_000)
    public void testScanFirstResult() throws Throwable {

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");

        String TEST_TABLE = "testGetFirstResult";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Transaction tx1 = tm.begin();

        Put row1 = new Put(rowName1);
        row1.add(famName1, colName1, dataValue1);
        tt.put(tx1, row1);

        tm.commit(tx1);

        Transaction tx2 = tm.begin();

        ResultScanner iterableRS = tt.getScanner(tx2, new Scan().setStartRow(rowName1).setStopRow(rowName1));
        Result result = iterableRS.next();
        long tsRow = result.rawCells()[0].getTimestamp();
        assertEquals(tsRow, tx1.getTransactionId(), "Reading differnt version");

        assertFalse(iterableRS.next() != null);

        tm.commit(tx2);

        Transaction tx3 = tm.begin();

        Put put3 = new Put(rowName1);
        put3.add(famName1, colName1, dataValue1);
        tt.put(tx3, put3);

        tm.commit(tx3);

        Transaction tx4 = tm.begin();

        ResultScanner iterableRS2 = tt.getScanner(tx4, new Scan().setStartRow(rowName1).setStopRow(rowName1));
        Result result2 = iterableRS2.next();
        long tsRow2 = result2.rawCells()[0].getTimestamp();
        assertEquals(tsRow2, tx3.getTransactionId(), "Reading differnt version");

        assertFalse(iterableRS2.next() != null);

        tm.commit(tx4);

        tt.close();
    }

    @Test(timeOut = 60_000)
    public void testScanSecondResult() throws Throwable {

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");

        String TEST_TABLE = "testGetFirstResult";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Transaction tx1 = tm.begin();

        Put put1 = new Put(rowName1);
        put1.add(famName1, colName1, dataValue1);
        tt.put(tx1, put1);

        tm.commit(tx1);

        Transaction tx2 = tm.begin();

        Put put2 = new Put(rowName1);
        put2.add(famName1, colName1, dataValue1);
        tt.put(tx2, put2);

        Transaction tx3 = tm.begin();

        ResultScanner iterableRS = tt.getScanner(tx3, new Scan().setStartRow(rowName1).setStopRow(rowName1));
        Result result = iterableRS.next();
        long tsRow = result.rawCells()[0].getTimestamp();
        assertEquals(tsRow, tx1.getTransactionId(), "Reading differnt version");

        assertFalse(iterableRS.next() != null);

        tm.commit(tx3);

        tt.close();
    }

    @Test (timeOut = 60_000)
    public void testScanFewResults() throws Throwable {

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] rowName2 = Bytes.toBytes("row2");
        byte[] rowName3 = Bytes.toBytes("row3");
        byte[] famName = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] colName2 = Bytes.toBytes("col2");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");
        byte[] dataValue2 = Bytes.toBytes("testWrite-2");

        String TEST_TABLE = "testGetFirstResult";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Transaction tx1 = tm.begin();

        Put put1 = new Put(rowName1);
        put1.add(famName, colName1, dataValue1);
        tt.put(tx1, put1);

        tm.commit(tx1);

        Transaction tx2 = tm.begin();

        Put put2 = new Put(rowName2);
        put2.add(famName, colName2, dataValue2);
        tt.put(tx2, put2);

        tm.commit(tx2);

        Transaction tx3 = tm.begin();

        ResultScanner iterableRS = tt.getScanner(tx3, new Scan().setStartRow(rowName1).setStopRow(rowName3));
        Result result = iterableRS.next();
        long tsRow = result.rawCells()[0].getTimestamp();
        assertEquals(tsRow, tx1.getTransactionId(), "Reading differnt version");

        result = iterableRS.next();
        tsRow = result.rawCells()[0].getTimestamp();
        assertEquals(tsRow, tx2.getTransactionId(), "Reading differnt version");

        assertFalse(iterableRS.next() != null);

        tm.commit(tx3);

        tt.close();
    }

    @Test (timeOut = 60_000)
    public void testScanFewResultsDifferentTransaction() throws Throwable {

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] rowName2 = Bytes.toBytes("row2");
        byte[] rowName3 = Bytes.toBytes("row3");
        byte[] famName = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] colName2 = Bytes.toBytes("col2");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");
        byte[] dataValue2 = Bytes.toBytes("testWrite-2");

        String TEST_TABLE = "testGetFirstResult";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Transaction tx1 = tm.begin();

        Put put1 = new Put(rowName1);
        put1.add(famName, colName1, dataValue1);
        tt.put(tx1, put1);
        Put put2 = new Put(rowName2);
        put2.add(famName, colName2, dataValue2);
        tt.put(tx1, put2);

        tm.commit(tx1);

        Transaction tx2 = tm.begin();

        put2 = new Put(rowName2);
        put2.add(famName, colName2, dataValue2);
        tt.put(tx2, put2);

        tm.commit(tx2);

        Transaction tx3 = tm.begin();

        ResultScanner iterableRS = tt.getScanner(tx3, new Scan().setStartRow(rowName1).setStopRow(rowName3));
        Result result = iterableRS.next();
        long tsRow = result.rawCells()[0].getTimestamp();
        assertEquals(tsRow, tx1.getTransactionId(), "Reading differnt version");

        result = iterableRS.next();
        tsRow = result.rawCells()[0].getTimestamp();
        assertEquals(tsRow, tx2.getTransactionId(), "Reading differnt version");

        assertFalse(iterableRS.next() != null);

        tm.commit(tx3);

        tt.close();
    }

    @Test (timeOut = 60_000)
    public void testScanFewResultsSameTransaction() throws Throwable {

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] rowName2 = Bytes.toBytes("row2");
        byte[] rowName3 = Bytes.toBytes("row3");
        byte[] famName = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] colName2 = Bytes.toBytes("col2");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");
        byte[] dataValue2 = Bytes.toBytes("testWrite-2");

        String TEST_TABLE = "testGetFirstResult";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Transaction tx1 = tm.begin();

        Put put1 = new Put(rowName1);
        put1.add(famName, colName1, dataValue1);
        tt.put(tx1, put1);
        Put put2 = new Put(rowName2);
        put2.add(famName, colName2, dataValue2);
        tt.put(tx1, put2);

        tm.commit(tx1);

        Transaction tx2 = tm.begin();

        put2 = new Put(rowName2);
        put2.add(famName, colName2, dataValue2);
        tt.put(tx2, put2);

        Transaction tx3 = tm.begin();

        ResultScanner iterableRS = tt.getScanner(tx3, new Scan().setStartRow(rowName1).setStopRow(rowName3));
        Result result = iterableRS.next();
        long tsRow = result.rawCells()[0].getTimestamp();
        assertEquals(tsRow, tx1.getTransactionId(), "Reading differnt version");

        result = iterableRS.next();
        tsRow = result.rawCells()[0].getTimestamp();
        assertEquals(tsRow, tx1.getTransactionId(), "Reading differnt version");

        assertFalse(iterableRS.next() != null);

        tm.commit(tx3);

        tt.close();
    }
}
