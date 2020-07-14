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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.TestUtils;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.hbase.HBaseCommitTableConfig;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.timestamp.storage.HBaseTimestampStorageConfig;
import org.apache.omid.tso.TSOServer;
import org.apache.omid.tso.TSOServerConfig;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import static org.testng.Assert.fail;

import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestSnapshotFilter {

    private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotFilter.class);

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
        tsoConfig.setPort(5679);
        tsoConfig.setConflictMapSize(1);
        tsoConfig.setWaitStrategy("LOW_CPU");
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
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));

            for (byte[] family : families) {
                HColumnDescriptor datafam = new HColumnDescriptor(family);
                datafam.setMaxVersions(MAX_VERSIONS);
                desc.addFamily(datafam);
            }

            int priority = Coprocessor.PRIORITY_HIGHEST;

            desc.addCoprocessor(OmidSnapshotFilter.class.getName(),null,++priority,null);
            desc.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation",null,++priority,null);

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
        TestUtils.waitForSocketListening("localhost", 5679, 100);
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
        TestUtils.waitForSocketNotListening("localhost", 5679, 1000);
    }

    @BeforeMethod
    public void setupTestSnapshotFilterIndividualTest() throws Exception {
        tm = spy((AbstractTransactionManager) newTransactionManager());
    }

    private TransactionManager newTransactionManager() throws Exception {
        HBaseOmidClientConfiguration hbaseOmidClientConf = new HBaseOmidClientConfiguration();
        hbaseOmidClientConf.setConnectionString("localhost:5679");
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
    public void testGetFirstResult() throws Throwable {
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
        put3.addColumn(famName1, colName1, dataValue1);
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


    // This test will fail if filtering is done before snapshot filtering
    @Test(timeOut = 60_000)
    public void testServerSideSnapshotFiltering() throws Throwable {
        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");
        byte[] dataValue2 = Bytes.toBytes("testWrite-2");

        String TEST_TABLE = "testServerSideSnapshotFiltering";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));

        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction tx1 = tm.begin();
        Put put1 = new Put(rowName1);
        put1.addColumn(famName1, colName1, dataValue1);
        tt.put(tx1, put1);
        tm.commit(tx1);

        Transaction tx2 = tm.begin();
        Put put2 = new Put(rowName1);
        put2.addColumn(famName1, colName1, dataValue2);
        tt.put(tx2, put2);

        Transaction tx3 = tm.begin();
        Get get = new Get(rowName1);

        // If snapshot filtering is not done in the server then the first value is
        // "testWrite-2" and the whole row will be filtered out.
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                famName1,
                colName1,
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("testWrite-1"));

        get.setFilter(filter);
        Result results = tt.get(tx3, get);
        assertTrue(results.size() == 1);
    }


    // This test will fail if filtering is done before snapshot filtering
    @Test(timeOut = 60_000)
    public void testServerSideSnapshotScannerFiltering() throws Throwable {
        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");
        byte[] dataValue2 = Bytes.toBytes("testWrite-2");

        String TEST_TABLE = "testServerSideSnapshotFiltering";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));

        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction tx1 = tm.begin();
        Put put1 = new Put(rowName1);
        put1.addColumn(famName1, colName1, dataValue1);
        tt.put(tx1, put1);
        tm.commit(tx1);

        Transaction tx2 = tm.begin();
        Put put2 = new Put(rowName1);
        put2.addColumn(famName1, colName1, dataValue2);
//        tt.put(tx2, put2);

        Transaction tx3 = tm.begin();

        // If snapshot filtering is not done in the server then the first value is
        // "testWrite-2" and the whole row will be filtered out.
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                famName1,
                colName1,
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("testWrite-1"));


        Scan scan = new Scan();
        scan.setFilter(filter);

        ResultScanner iterableRS = tt.getScanner(tx3, scan);
        Result result = iterableRS.next();

        assertTrue(result.size() == 1);
    }


    @Test(timeOut = 60_000)
    public void testGetWithFamilyDelete() throws Throwable {
        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] famName2 = Bytes.toBytes("test-fam2");
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] colName2 = Bytes.toBytes("col2");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");

        String TEST_TABLE = "testGetWithFamilyDelete";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY), famName2);

        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction tx1 = tm.begin();

        Put put1 = new Put(rowName1);
        put1.addColumn(famName1, colName1, dataValue1);
        tt.put(tx1, put1);

        tm.commit(tx1);

        Transaction tx2 = tm.begin();
        Put put2 = new Put(rowName1);
        put2.addColumn(famName2, colName2, dataValue1);
        tt.put(tx2, put2);
        tm.commit(tx2);

        Transaction tx3 = tm.begin();

        Delete d = new Delete(rowName1);
        d.addFamily(famName2);
        tt.delete(tx3, d);


        Transaction tx4 = tm.begin();

        Get get = new Get(rowName1);

        Filter filter1 = new FilterList(FilterList.Operator.MUST_PASS_ONE,
                new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(TEST_FAMILY))),
                new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(famName2)));

        get.setFilter(filter1);
        Result result = tt.get(tx4, get);
        assertTrue(result.size() == 2, "Result should be 2");

        try {
            tm.commit(tx3);
        } catch (RollbackException e) {
            if (!tm.isLowLatency())
                fail();
        }
        Transaction tx5 = tm.begin();
        result = tt.get(tx5, get);
        if (!tm.isLowLatency())
            assertTrue(result.size() == 1, "Result should be 1");

        tt.close();
    }

    @Test(timeOut = 60_000)
    public void testReadFromCommitTable() throws Exception {
        final byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");
        final String TEST_TABLE = "testReadFromCommitTable";
        final byte[] famName2 = Bytes.toBytes("test-fam2");

        final CountDownLatch readAfterCommit = new CountDownLatch(1);
        final CountDownLatch postCommitBegin = new CountDownLatch(1);

        final AtomicBoolean readFailed = new AtomicBoolean(false);
        final AbstractTransactionManager tm = (AbstractTransactionManager) newTransactionManager();
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY), famName2);

        doAnswer(new Answer<ListenableFuture<Void>>() {
            @Override
            public ListenableFuture<Void> answer(InvocationOnMock invocation) throws Throwable {
                LOG.info("Releasing readAfterCommit barrier");
                readAfterCommit.countDown();
                LOG.info("Waiting postCommitBegin barrier");
                postCommitBegin.await();
                ListenableFuture<Void> result = (ListenableFuture<Void>) invocation.callRealMethod();
                return result;
            }
        }).when(syncPostCommitter).updateShadowCells(any(HBaseTransaction.class));

        Thread readThread = new Thread("Read Thread") {
            @Override
            public void run() {

                try {
                    LOG.info("Waiting readAfterCommit barrier");
                    readAfterCommit.await();

                    Transaction tx4 = tm.begin();
                    TTable tt = new TTable(connection, TEST_TABLE);
                    Get get = new Get(rowName1);

                    Filter filter1 = new FilterList(FilterList.Operator.MUST_PASS_ONE,
                            new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(TEST_FAMILY))),
                            new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(famName2)));

                    get.setFilter(filter1);
                    Result result = tt.get(tx4, get);

                    if (result.size() == 2) {
                        readFailed.set(false);
                    }
                    else {
                        readFailed.set(false);
                    }

                    postCommitBegin.countDown();
                } catch (Throwable e) {
                    readFailed.set(false);
                    LOG.error("Error whilst reading", e);
                }
            }
        };
        readThread.start();

        TTable table = new TTable(connection, TEST_TABLE);
        final HBaseTransaction t1 = (HBaseTransaction) tm.begin();
        Put put1 = new Put(rowName1);
        put1.addColumn(famName1, colName1, dataValue1);
        table.put(t1, put1);
        tm.commit(t1);

        readThread.join();

        assertFalse(readFailed.get(), "Read should have succeeded");

    }



    @Test(timeOut = 60_000)
    public void testGetWithFilter() throws Throwable {
        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] famName2 = Bytes.toBytes("test-fam2");
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] colName2 = Bytes.toBytes("col2");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");

        String TEST_TABLE = "testGetWithFilter";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY), famName2);
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction tx1 = tm.begin();

        Put put1 = new Put(rowName1);
        put1.addColumn(famName1, colName1, dataValue1);
        tt.put(tx1, put1);

        tm.commit(tx1);

        Transaction tx2 = tm.begin();
        Put put2 = new Put(rowName1);
        put2.addColumn(famName2, colName2, dataValue1);
        tt.put(tx2, put2);
        tm.commit(tx2);

        Transaction tx3 = tm.begin();

        Get get = new Get(rowName1);

        Filter filter1 = new FilterList(FilterList.Operator.MUST_PASS_ONE,
                new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(TEST_FAMILY))),
                new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(famName2)));

        get.setFilter(filter1);
        Result result = tt.get(tx3, get);
        assertTrue(result.size() == 2, "Result should be 2");


        Filter filter2 = new FilterList(FilterList.Operator.MUST_PASS_ONE,
                new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(TEST_FAMILY))));

        get.setFilter(filter2);
        result = tt.get(tx3, get);
        assertTrue(result.size() == 1, "Result should be 2");

        tm.commit(tx3);

        tt.close();
    }


    @Test(timeOut = 60_000)
    public void testGetSecondResult() throws Throwable {
        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");

        String TEST_TABLE = "testGetSecondResult";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction tx1 = tm.begin();

        Put put1 = new Put(rowName1);
        put1.addColumn(famName1, colName1, dataValue1);
        tt.put(tx1, put1);

        tm.commit(tx1);

        Transaction tx2 = tm.begin();
        Put put2 = new Put(rowName1);
        put2.addColumn(famName1, colName1, dataValue1);
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

        String TEST_TABLE = "testScanFirstResult";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction tx1 = tm.begin();

        Put row1 = new Put(rowName1);
        row1.addColumn(famName1, colName1, dataValue1);
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
        put3.addColumn(famName1, colName1, dataValue1);
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
    public void testScanWithFilter() throws Throwable {

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] famName2 = Bytes.toBytes("test-fam2");
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] colName2 = Bytes.toBytes("col2");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");

        String TEST_TABLE = "testScanWithFilter";
        createTableIfNotExists(TEST_TABLE, famName1, famName2);
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction tx1 = tm.begin();
        Put put1 = new Put(rowName1);
        put1.addColumn(famName1, colName1, dataValue1);
        tt.put(tx1, put1);
        tm.commit(tx1);

        Transaction tx2 = tm.begin();
        Put put2 = new Put(rowName1);
        put2.addColumn(famName2, colName2, dataValue1);
        tt.put(tx2, put2);

        tm.commit(tx2);
        Transaction tx3 = tm.begin();

        Scan scan = new Scan();
        scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE,
                new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(TEST_FAMILY)))));
        scan.setStartRow(rowName1).setStopRow(rowName1);

        ResultScanner iterableRS = tt.getScanner(tx3, scan);
        Result result = iterableRS.next();
        assertTrue(result.containsColumn(famName1, colName1));
        assertFalse(result.containsColumn(famName2, colName2));

        scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE,
                new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(TEST_FAMILY))),
                new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(famName2))));

        iterableRS = tt.getScanner(tx3, scan);
        result = iterableRS.next();
        assertTrue(result.containsColumn(famName1, colName1));
        assertTrue(result.containsColumn(famName2, colName2));

        tm.commit(tx3);
        tt.close();
    }


    @Test(timeOut = 60_000)
    public void testScanSecondResult() throws Throwable {

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");

        String TEST_TABLE = "testScanSecondResult";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction tx1 = tm.begin();

        Put put1 = new Put(rowName1);
        put1.addColumn(famName1, colName1, dataValue1);
        tt.put(tx1, put1);

        tm.commit(tx1);

        Transaction tx2 = tm.begin();

        Put put2 = new Put(rowName1);
        put2.addColumn(famName1, colName1, dataValue1);
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

        String TEST_TABLE = "testScanFewResults";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction tx1 = tm.begin();

        Put put1 = new Put(rowName1);
        put1.addColumn(famName, colName1, dataValue1);
        tt.put(tx1, put1);

        tm.commit(tx1);

        Transaction tx2 = tm.begin();

        Put put2 = new Put(rowName2);
        put2.addColumn(famName, colName2, dataValue2);
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

        String TEST_TABLE = "testScanFewResultsDifferentTransaction";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction tx1 = tm.begin();

        Put put1 = new Put(rowName1);
        put1.addColumn(famName, colName1, dataValue1);
        tt.put(tx1, put1);
        Put put2 = new Put(rowName2);
        put2.addColumn(famName, colName2, dataValue2);
        tt.put(tx1, put2);

        tm.commit(tx1);

        Transaction tx2 = tm.begin();

        put2 = new Put(rowName2);
        put2.addColumn(famName, colName2, dataValue2);
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

        String TEST_TABLE = "testScanFewResultsSameTransaction";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction tx1 = tm.begin();

        Put put1 = new Put(rowName1);
        put1.addColumn(famName, colName1, dataValue1);
        tt.put(tx1, put1);
        Put put2 = new Put(rowName2);
        put2.addColumn(famName, colName2, dataValue2);
        tt.put(tx1, put2);

        tm.commit(tx1);

        Transaction tx2 = tm.begin();

        put2 = new Put(rowName2);
        put2.addColumn(famName, colName2, dataValue2);
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


    @Test (timeOut = 60_000)
    public void testFilterCommitCacheInSnapshot() throws Throwable {
        String TEST_TABLE = "testFilterCommitCacheInSnapshot";
        byte[] rowName = Bytes.toBytes("row1");
        byte[] famName = Bytes.toBytes(TEST_FAMILY);

        createTableIfNotExists(TEST_TABLE, famName);
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction tx1 = tm.begin();
        Put put = new Put(rowName);
        for (int i = 0; i < 200; ++i) {
            byte[] dataValue1 = Bytes.toBytes("some data");
            byte[] colName = Bytes.toBytes("col" + i);
            put.addColumn(famName, colName, dataValue1);
        }
        tt.put(tx1, put);
        tm.commit(tx1);
        Transaction tx3 = tm.begin();

        Table htable = connection.getTable(TableName.valueOf(TEST_TABLE));
        SnapshotFilterImpl snapshotFilter = spy(new SnapshotFilterImpl(new HTableAccessWrapper(htable, htable),
                tm.getCommitTableClient()));
        Filter newFilter = TransactionFilters.getVisibilityFilter(null,
                snapshotFilter, (HBaseTransaction) tx3);

        Table rawTable = connection.getTable(TableName.valueOf(TEST_TABLE));

        Scan scan = new Scan();
        ResultScanner scanner = rawTable.getScanner(scan);

        for(Result row: scanner) {
            for(Cell cell: row.rawCells()) {
                newFilter.filterKeyValue(cell);

            }
        }
        verify(snapshotFilter, Mockito.times(0))
                .getTSIfInSnapshot(any(Cell.class),any(HBaseTransaction.class), any(Map.class));
        tm.commit(tx3);
        tt.close();
    }

    @Test (timeOut = 60_000)
    public void testFilterCommitCacheNotInSnapshot() throws Throwable {
        String TEST_TABLE = "testFilterCommitCacheNotInSnapshot";
        byte[] rowName = Bytes.toBytes("row1");
        byte[] famName = Bytes.toBytes(TEST_FAMILY);

        createTableIfNotExists(TEST_TABLE, famName);
        TTable tt = new TTable(connection, TEST_TABLE);


        //add some uncommitted values
        Transaction tx1 = tm.begin();
        Put put = new Put(rowName);
        for (int i = 0; i < 200; ++i) {
            byte[] dataValue1 = Bytes.toBytes("some data");
            byte[] colName = Bytes.toBytes("col" + i);
            put.addColumn(famName, colName, dataValue1);
        }
        tt.put(tx1, put);

        //try to scan from tx
        Transaction tx = tm.begin();
        Table htable = connection.getTable(TableName.valueOf(TEST_TABLE));
        SnapshotFilterImpl snapshotFilter = spy(new SnapshotFilterImpl(new HTableAccessWrapper(htable, htable),
                tm.getCommitTableClient()));
        Filter newFilter = TransactionFilters.getVisibilityFilter(null,
                snapshotFilter, (HBaseTransaction) tx);

        Table rawTable = connection.getTable(TableName.valueOf(TEST_TABLE));

        Scan scan = new Scan();
        ResultScanner scanner = rawTable.getScanner(scan);

        for(Result row: scanner) {
            for(Cell cell: row.rawCells()) {
                newFilter.filterKeyValue(cell);
            }
        }
        verify(snapshotFilter, Mockito.times(1))
                .getTSIfInSnapshot(any(Cell.class),any(HBaseTransaction.class), any(Map.class));
        tt.close();
    }


}
