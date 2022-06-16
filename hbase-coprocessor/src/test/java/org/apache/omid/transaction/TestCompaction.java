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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.TestUtils;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.hbase.HBaseCommitTableConfig;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.timestamp.storage.HBaseTimestampStorageConfig;
import org.apache.omid.tso.TSOServer;
import org.apache.omid.tso.TSOServerConfig;
import org.apache.omid.tso.client.OmidClientConfiguration;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestCompaction {

    private static final Logger LOG = LoggerFactory.getLogger(TestCompaction.class);

    private static final String TEST_FAMILY = "test-fam";
    private static final String TEST_QUALIFIER = "test-qual";

    private final byte[] fam = Bytes.toBytes(TEST_FAMILY);
    private final byte[] qual = Bytes.toBytes(TEST_QUALIFIER);
    private final byte[] data = Bytes.toBytes("testWrite-1");

    private static final int MAX_VERSIONS = 3;

    private Random randomGenerator;
    private AbstractTransactionManager tm;

    private Injector injector;

    private Admin admin;
    private Configuration hbaseConf;
    private HBaseTestingUtility hbaseTestUtil;
    private MiniHBaseCluster hbaseCluster;

    private TSOServer tso;


    private CommitTable commitTable;
    private PostCommitActions syncPostCommitter;
    private static Connection connection;

    @BeforeClass
    public void setupTestCompation() throws Exception {
        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setPort(1234);
        tsoConfig.setConflictMapSize(1);
        tsoConfig.setWaitStrategy("LOW_CPU");
        injector = Guice.createInjector(new TSOForHBaseCompactorTestModule(tsoConfig));
        hbaseConf = injector.getInstance(Configuration.class);
        HBaseCommitTableConfig hBaseCommitTableConfig = injector.getInstance(HBaseCommitTableConfig.class);
        HBaseTimestampStorageConfig hBaseTimestampStorageConfig = injector.getInstance(HBaseTimestampStorageConfig.class);

        // settings required for #testDuplicateDeletes()
        hbaseConf.setInt("hbase.hstore.compaction.min", 2);
        hbaseConf.setInt("hbase.hstore.compaction.max", 2);
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

            desc.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation",null,Coprocessor.PRIORITY_HIGHEST,null);
            admin.createTable(desc);
            for (byte[] family : families) {
                CompactorUtil.enableOmidCompaction(connection, TableName.valueOf(tableName), family);
            }
        }

    }

    private void setupTSO() throws IOException, InterruptedException {
        tso = injector.getInstance(TSOServer.class);
        tso.startAsync();
        tso.awaitRunning();
        TestUtils.waitForSocketListening("localhost", 1234, 100);
        Thread.currentThread().setName("UnitTest(s) thread");
    }

    @AfterClass
    public void cleanupTestCompation() throws Exception {
        teardownTSO();
        hbaseCluster.shutdown();
    }

    private void teardownTSO() throws IOException, InterruptedException {
        tso.stopAsync();
        tso.awaitTerminated();
        TestUtils.waitForSocketNotListening("localhost", 1234, 1000);
    }

    @BeforeMethod
    public void setupTestCompactionIndividualTest() throws Exception {
        randomGenerator = new Random(0xfeedcafeL);
        tm = spy((AbstractTransactionManager) newTransactionManager());
    }

    private TransactionManager newTransactionManager() throws Exception {
        HBaseOmidClientConfiguration hbaseOmidClientConf = new HBaseOmidClientConfiguration();
        hbaseOmidClientConf.setConnectionString("localhost:1234");
        hbaseOmidClientConf.setHBaseConfiguration(hbaseConf);
        CommitTable.Client commitTableClient = commitTable.getClient();
        syncPostCommitter =
                spy(new HBaseSyncPostCommitter(new NullMetricsProvider(),commitTableClient, connection));
        return HBaseTransactionManager.builder(hbaseOmidClientConf)
                .postCommitter(syncPostCommitter)
                .commitTableClient(commitTableClient)
                .build();
    }


    @Test
    public void testShadowCellsAboveLWMSurviveCompaction() throws Exception {
        String TEST_TABLE = "testShadowCellsAboveLWMSurviveCompaction";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(connection, TEST_TABLE);

        byte[] rowId = Bytes.toBytes("row");

        // Create 3 transactions modifying the same cell in a particular row
        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        Put put1 = new Put(rowId);
        put1.addColumn(fam, qual, Bytes.toBytes("testValue 1"));
        txTable.put(tx1, put1);
        tm.commit(tx1);

        HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
        Put put2 = new Put(rowId);
        put2.addColumn(fam, qual, Bytes.toBytes("testValue 2"));
        txTable.put(tx2, put2);
        tm.commit(tx2);

        HBaseTransaction tx3 = (HBaseTransaction) tm.begin();
        Put put3 = new Put(rowId);
        put3.addColumn(fam, qual, Bytes.toBytes("testValue 3"));
        txTable.put(tx3, put3);
        tm.commit(tx3);

        // Before compaction, the three timestamped values for the cell should be there
        TTableCellGetterAdapter getter = new TTableCellGetterAdapter(txTable);
        assertTrue(CellUtils.hasCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                "Put cell of Tx1 should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                "Put shadow cell of Tx1 should be there");
        assertTrue(CellUtils.hasCell(rowId, fam, qual, tx2.getStartTimestamp(), getter),
                "Put cell of Tx2 cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, tx2.getStartTimestamp(), getter),
                "Put shadow cell of Tx2 should be there");
        assertTrue(CellUtils.hasCell(rowId, fam, qual, tx3.getStartTimestamp(), getter),
                "Put cell of Tx3 cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, tx3.getStartTimestamp(), getter),
                "Put shadow cell of Tx3 should be there");

        // Compact
        compactWithLWM(0, TEST_TABLE);

        // After compaction, the three timestamped values for the cell should be there
        assertTrue(CellUtils.hasCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                "Put cell of Tx1 should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                "Put shadow cell of Tx1 should be there");
        assertTrue(CellUtils.hasCell(rowId, fam, qual, tx2.getStartTimestamp(), getter),
                "Put cell of Tx2 cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, tx2.getStartTimestamp(), getter),
                "Put shadow cell of Tx2 should be there");
        assertTrue(CellUtils.hasCell(rowId, fam, qual, tx3.getStartTimestamp(), getter),
                "Put cell of Tx3 cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, tx3.getStartTimestamp(), getter),
                "Put shadow cell of Tx3 should be there");
    }

    @Test(timeOut = 60_000)
    public void testStandardTXsWithShadowCellsAndWithSTBelowAndAboveLWMArePresevedAfterCompaction() throws Throwable {
        String TEST_TABLE = "testStandardTXsWithShadowCellsAndWithSTBelowAndAboveLWMArePresevedAfterCompaction";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(connection, TEST_TABLE);

        final int ROWS_TO_ADD = 5;

        long fakeAssignedLowWatermark = 0L;
        for (int i = 0; i < ROWS_TO_ADD; ++i) {
            long rowId = randomGenerator.nextLong();
            Transaction tx = tm.begin();
            if (i == (ROWS_TO_ADD / 2)) {
                fakeAssignedLowWatermark = tx.getTransactionId();
                LOG.info("AssignedLowWatermark " + fakeAssignedLowWatermark);
            }
            Put put = new Put(Bytes.toBytes(rowId));
            put.addColumn(fam, qual, data);
            txTable.put(tx, put);
            tm.commit(tx);
        }

        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TableName.valueOf(TEST_TABLE));

        // Return a LWM that triggers compaction & stays between 1 and the max start timestamp assigned to previous TXs
        LOG.info("Regions in table {}: {}", TEST_TABLE, hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).size());
        OmidCompactor omidCompactor = (OmidCompactor) hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).get(0)
                .getCoprocessorHost().findCoprocessor(OmidCompactor.class.getName());
        CommitTable commitTable = injector.getInstance(CommitTable.class);
        CommitTable.Client commitTableClient = spy(commitTable.getClient());
        SettableFuture<Long> f = SettableFuture.create();
        f.set(fakeAssignedLowWatermark);
        doReturn(f).when(commitTableClient).readLowWatermark();
        omidCompactor.commitTableClient = commitTableClient;
        LOG.info("Compacting table {}", TEST_TABLE);
        admin.majorCompact(TableName.valueOf(TEST_TABLE));

        LOG.info("Sleeping for 3 secs");
        Thread.sleep(3000);
        LOG.info("Waking up after 3 secs");

        // No rows should have been discarded after compacting
        assertEquals(rowCount(TEST_TABLE, fam), ROWS_TO_ADD, "Rows in table after compacting should be " + ROWS_TO_ADD);
    }

    @Test(timeOut = 60_000)
    public void testTXWithoutShadowCellsAndWithSTBelowLWMGetsShadowCellHealedAfterCompaction() throws Exception {
        String TEST_TABLE = "testTXWithoutShadowCellsAndWithSTBelowLWMGetsShadowCellHealedAfterCompaction";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(connection, TEST_TABLE);

        // The following line emulates a crash after commit that is observed in (*) below
        doThrow(new RuntimeException()).when(syncPostCommitter).updateShadowCells(any(HBaseTransaction.class));

        HBaseTransaction problematicTx = (HBaseTransaction) tm.begin();

        long row = randomGenerator.nextLong();

        // Test shadow cell are created properly
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(fam, qual, data);
        txTable.put(problematicTx, put);
        try {
            tm.commit(problematicTx);
        } catch (Exception e) { // (*) Crash
            // Do nothing
        }

        assertTrue(CellUtils.hasCell(Bytes.toBytes(row), fam, qual, problematicTx.getStartTimestamp(),
                                     new TTableCellGetterAdapter(txTable)),
                   "Cell should be there");
        assertFalse(CellUtils.hasShadowCell(Bytes.toBytes(row), fam, qual, problematicTx.getStartTimestamp(),
                                            new TTableCellGetterAdapter(txTable)),
                    "Shadow cell should not be there");

        // Return a LWM that triggers compaction and has all the possible start timestamps below it
        LOG.info("Regions in table {}: {}", TEST_TABLE, hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).size());
        OmidCompactor omidCompactor = (OmidCompactor) hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).get(0)
                .getCoprocessorHost().findCoprocessor(OmidCompactor.class.getName());
        CommitTable commitTable = injector.getInstance(CommitTable.class);
        CommitTable.Client commitTableClient = spy(commitTable.getClient());
        SettableFuture<Long> f = SettableFuture.create();
        f.set(Long.MAX_VALUE);
        doReturn(f).when(commitTableClient).readLowWatermark();
        omidCompactor.commitTableClient = commitTableClient;

        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TableName.valueOf(TEST_TABLE));

        LOG.info("Compacting table {}", TEST_TABLE);
        admin.majorCompact(TableName.valueOf(TEST_TABLE));

        LOG.info("Sleeping for 3 secs");
        Thread.sleep(3000);
        LOG.info("Waking up after 3 secs");

        assertTrue(CellUtils.hasCell(Bytes.toBytes(row), fam, qual, problematicTx.getStartTimestamp(),
                                     new TTableCellGetterAdapter(txTable)),
                   "Cell should be there");
        assertTrue(CellUtils.hasShadowCell(Bytes.toBytes(row), fam, qual, problematicTx.getStartTimestamp(),
                                           new TTableCellGetterAdapter(txTable)),
                   "Shadow cell should not be there");
    }

    @Test(timeOut = 60_000)
    public void testNeverendingTXsWithSTBelowAndAboveLWMAreDiscardedAndPreservedRespectivelyAfterCompaction()
            throws Throwable {
        String
                TEST_TABLE =
                "testNeverendingTXsWithSTBelowAndAboveLWMAreDiscardedAndPreservedRespectivelyAfterCompaction";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(connection, TEST_TABLE);

        // The KV in this transaction should be discarded
        HBaseTransaction neverendingTxBelowLowWatermark = (HBaseTransaction) tm.begin();
        long rowId = randomGenerator.nextLong();
        Put put = new Put(Bytes.toBytes(rowId));
        put.addColumn(fam, qual, data);
        txTable.put(neverendingTxBelowLowWatermark, put);
        assertTrue(CellUtils.hasCell(Bytes.toBytes(rowId), fam, qual, neverendingTxBelowLowWatermark.getStartTimestamp(),
                                     new TTableCellGetterAdapter(txTable)),
                   "Cell should be there");
        assertFalse(CellUtils.hasShadowCell(Bytes.toBytes(rowId), fam, qual, neverendingTxBelowLowWatermark.getStartTimestamp(),
                                            new TTableCellGetterAdapter(txTable)),
                    "Shadow cell should not be there");

        // The KV in this transaction should be added without the shadow cells
        HBaseTransaction neverendingTxAboveLowWatermark = (HBaseTransaction) tm.begin();
        long anotherRowId = randomGenerator.nextLong();
        put = new Put(Bytes.toBytes(anotherRowId));
        put.addColumn(fam, qual, data);
        txTable.put(neverendingTxAboveLowWatermark, put);
        assertTrue(CellUtils.hasCell(Bytes.toBytes(anotherRowId), fam, qual, neverendingTxAboveLowWatermark.getStartTimestamp(),
                                     new TTableCellGetterAdapter(txTable)),
                   "Cell should be there");
        assertFalse(CellUtils.hasShadowCell(Bytes.toBytes(anotherRowId), fam, qual, neverendingTxAboveLowWatermark.getStartTimestamp(),
                                            new TTableCellGetterAdapter(txTable)),
                    "Shadow cell should not be there");

        assertEquals(rowCount(TEST_TABLE, fam), 2, "Rows in table before flushing should be 2");
        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TableName.valueOf(TEST_TABLE));
        assertEquals(rowCount(TEST_TABLE, fam), 2, "Rows in table after flushing should be 2");

        // Return a LWM that triggers compaction and stays between both ST of TXs, so assign 1st TX's start timestamp
        LOG.info("Regions in table {}: {}", TEST_TABLE, hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).size());
        OmidCompactor omidCompactor = (OmidCompactor) hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).get(0)
                .getCoprocessorHost().findCoprocessor(OmidCompactor.class.getName());
        CommitTable commitTable = injector.getInstance(CommitTable.class);
        CommitTable.Client commitTableClient = spy(commitTable.getClient());
        SettableFuture<Long> f = SettableFuture.create();
        f.set(neverendingTxBelowLowWatermark.getStartTimestamp());
        doReturn(f).when(commitTableClient).readLowWatermark();
        omidCompactor.commitTableClient = commitTableClient;
        LOG.info("Compacting table {}", TEST_TABLE);
        admin.majorCompact(TableName.valueOf(TEST_TABLE));

        LOG.info("Sleeping for 3 secs");
        Thread.sleep(3000);
        LOG.info("Waking up after 3 secs");

        // One row should have been discarded after compacting
        assertEquals(rowCount(TEST_TABLE, fam), 1, "There should be only one row in table after compacting");
        // The row from the TX below the LWM should not be there (nor its Shadow Cell)
        assertFalse(CellUtils.hasCell(Bytes.toBytes(rowId), fam, qual, neverendingTxBelowLowWatermark.getStartTimestamp(),
                                      new TTableCellGetterAdapter(txTable)),
                    "Cell should not be there");
        assertFalse(CellUtils.hasShadowCell(Bytes.toBytes(rowId), fam, qual, neverendingTxBelowLowWatermark.getStartTimestamp(),
                                            new TTableCellGetterAdapter(txTable)),
                    "Shadow cell should not be there");
        // The row from the TX above the LWM should be there without the Shadow Cell
        assertTrue(CellUtils.hasCell(Bytes.toBytes(anotherRowId), fam, qual, neverendingTxAboveLowWatermark.getStartTimestamp(),
                                     new TTableCellGetterAdapter(txTable)),
                   "Cell should be there");
        assertFalse(CellUtils.hasShadowCell(Bytes.toBytes(anotherRowId), fam, qual, neverendingTxAboveLowWatermark.getStartTimestamp(),
                                            new TTableCellGetterAdapter(txTable)),
                    "Shadow cell should not be there");

    }

    @Test(timeOut = 60_000)
    public void testRowsUnalteredWhenCommitTableCannotBeReached() throws Throwable {
        String TEST_TABLE = "testRowsUnalteredWhenCommitTableCannotBeReached";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(connection, TEST_TABLE);

        // The KV in this transaction should be discarded but in the end should remain there because
        // the commit table won't be accessed (simulating an error on access)
        HBaseTransaction neverendingTx = (HBaseTransaction) tm.begin();
        long rowId = randomGenerator.nextLong();
        Put put = new Put(Bytes.toBytes(rowId));
        put.addColumn(fam, qual, data);
        txTable.put(neverendingTx, put);
        assertTrue(CellUtils.hasCell(Bytes.toBytes(rowId), fam, qual, neverendingTx.getStartTimestamp(),
                                     new TTableCellGetterAdapter(txTable)),
                   "Cell should be there");
        assertFalse(CellUtils.hasShadowCell(Bytes.toBytes(rowId), fam, qual, neverendingTx.getStartTimestamp(),
                                            new TTableCellGetterAdapter(txTable)),
                    "Shadow cell should not be there");

        assertEquals(rowCount(TEST_TABLE, fam), 1, "There should be only one rows in table before flushing");
        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TableName.valueOf(TEST_TABLE));
        assertEquals(rowCount(TEST_TABLE, fam), 1, "There should be only one rows in table after flushing");

        // Break access to CommitTable functionality in Compactor
        LOG.info("Regions in table {}: {}", TEST_TABLE, hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).size());
        OmidCompactor omidCompactor = (OmidCompactor) hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).get(0)
                .getCoprocessorHost().findCoprocessor(OmidCompactor.class.getName());
        CommitTable commitTable = injector.getInstance(CommitTable.class);
        CommitTable.Client commitTableClient = spy(commitTable.getClient());
        SettableFuture<Long> f = SettableFuture.create();
        f.setException(new IOException("Unable to read"));
        doReturn(f).when(commitTableClient).readLowWatermark();
        omidCompactor.commitTableClient = commitTableClient;
        LOG.info("Compacting table {}", TEST_TABLE);
        admin.majorCompact(TableName.valueOf(TEST_TABLE)); // Should trigger the error when accessing CommitTable funct.

        LOG.info("Sleeping for 3 secs");
        Thread.sleep(3000);
        LOG.info("Waking up after 3 secs");

        // All rows should be there after the failed compaction
        assertEquals(rowCount(TEST_TABLE, fam), 1, "There should be only one row in table after compacting");
        assertTrue(CellUtils.hasCell(Bytes.toBytes(rowId), fam, qual, neverendingTx.getStartTimestamp(),
                                     new TTableCellGetterAdapter(txTable)),
                   "Cell should be there");
        assertFalse(CellUtils.hasShadowCell(Bytes.toBytes(rowId), fam, qual, neverendingTx.getStartTimestamp(),
                                            new TTableCellGetterAdapter(txTable)),
                    "Shadow cell should not be there");
    }

    @Test(timeOut = 60_000)
    public void testOriginalTableParametersAreAvoidedAlsoWhenCompacting() throws Throwable {
        String TEST_TABLE = "testOriginalTableParametersAreAvoidedAlsoWhenCompacting";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(connection, TEST_TABLE);

        long rowId = randomGenerator.nextLong();
        for (int versionCount = 0; versionCount <= (2 * MAX_VERSIONS); versionCount++) {
            Transaction tx = tm.begin();
            Put put = new Put(Bytes.toBytes(rowId));
            put.addColumn(fam, qual, Bytes.toBytes("testWrite-" + versionCount));
            txTable.put(tx, put);
            tm.commit(tx);
        }

        Transaction tx = tm.begin();
        Get get = new Get(Bytes.toBytes(rowId));
        get.setMaxVersions(2 * MAX_VERSIONS);
        assertEquals(get.getMaxVersions(), (2 * MAX_VERSIONS), "Max versions should be set to " + (2 * MAX_VERSIONS));
        get.addColumn(fam, qual);
        Result result = txTable.get(tx, get);
        tm.commit(tx);
        List<Cell> column = result.getColumnCells(fam, qual);
        assertEquals(column.size(), 1, "There should be only one version in the result");

        assertEquals(rowCount(TEST_TABLE, fam), 1, "There should be only one row in table before flushing");
        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TableName.valueOf(TEST_TABLE));
        assertEquals(rowCount(TEST_TABLE, fam), 1, "There should be only one row in table after flushing");

        // Return a LWM that triggers compaction
        compactEverything(TEST_TABLE);

        // One row should have been discarded after compacting
        assertEquals(rowCount(TEST_TABLE, fam), 1, "There should be only one row in table after compacting");

        tx = tm.begin();
        get = new Get(Bytes.toBytes(rowId));
        get.setMaxVersions(2 * MAX_VERSIONS);
        assertEquals(get.getMaxVersions(), (2 * MAX_VERSIONS), "Max versions should be set to " + (2 * MAX_VERSIONS));
        get.addColumn(fam, qual);
        result = txTable.get(tx, get);
        tm.commit(tx);
        column = result.getColumnCells(fam, qual);
        assertEquals(column.size(), 1, "There should be only one version in the result");
        assertEquals(Bytes.toString(CellUtil.cloneValue(column.get(0))), "testWrite-" + (2 * MAX_VERSIONS),
                     "Values don't match");
    }

    // manually flush the regions on the region server.
    // flushing like this prevents compaction running
    // directly after the flush, which we want to avoid.
    private void manualFlush(String tableName) throws Throwable {
        LOG.info("Manually flushing all regions and waiting 2 secs");
        for (HRegion r : hbaseTestUtil.getHBaseCluster().getRegionServer(0).getRegions(TableName.valueOf(tableName))) {
            r.flush(true);
        }
        TimeUnit.SECONDS.sleep(2);
    }

    @Test(timeOut = 60_000)
    public void testOldCellsAreDiscardedAfterCompaction() throws Exception {
        String TEST_TABLE = "testOldCellsAreDiscardedAfterCompaction";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(connection, TEST_TABLE);

        byte[] rowId = Bytes.toBytes("row");

        // Create 3 transactions modifying the same cell in a particular row
        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        Put put1 = new Put(rowId);
        put1.addColumn(fam, qual, Bytes.toBytes("testValue 1"));
        txTable.put(tx1, put1);
        tm.commit(tx1);

        HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
        Put put2 = new Put(rowId);
        put2.addColumn(fam, qual, Bytes.toBytes("testValue 2"));
        txTable.put(tx2, put2);
        tm.commit(tx2);

        HBaseTransaction tx3 = (HBaseTransaction) tm.begin();
        Put put3 = new Put(rowId);
        put3.addColumn(fam, qual, Bytes.toBytes("testValue 3"));
        txTable.put(tx3, put3);
        tm.commit(tx3);

        // Before compaction, the three timestamped values for the cell should be there
        TTableCellGetterAdapter getter = new TTableCellGetterAdapter(txTable);
        assertTrue(CellUtils.hasCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                   "Put cell of Tx1 should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                   "Put shadow cell of Tx1 should be there");
        assertTrue(CellUtils.hasCell(rowId, fam, qual, tx2.getStartTimestamp(), getter),
                   "Put cell of Tx2 cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, tx2.getStartTimestamp(), getter),
                   "Put shadow cell of Tx2 should be there");
        assertTrue(CellUtils.hasCell(rowId, fam, qual, tx3.getStartTimestamp(), getter),
                   "Put cell of Tx3 cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, tx3.getStartTimestamp(), getter),
                   "Put shadow cell of Tx3 should be there");

        // Compact
        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();
        compactWithLWM(lwmTx.getStartTimestamp(), TEST_TABLE);

        // After compaction, only the last value for the cell should have survived
        assertFalse(CellUtils.hasCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                    "Put cell of Tx1 should not be there");
        assertFalse(CellUtils.hasShadowCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                    "Put shadow cell of Tx1 should not be there");
        assertFalse(CellUtils.hasCell(rowId, fam, qual, tx2.getStartTimestamp(), getter),
                    "Put cell of Tx2 should not be there");
        assertFalse(CellUtils.hasShadowCell(rowId, fam, qual, tx2.getStartTimestamp(), getter),
                    "Put shadow cell of Tx2 should not be there");
        assertTrue(CellUtils.hasCell(rowId, fam, qual, tx3.getStartTimestamp(), getter),
                   "Put cell of Tx3 cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, tx3.getStartTimestamp(), getter),
                   "Put shadow cell of Tx3 should be there");

        // A new transaction after compaction should read the last value written
        HBaseTransaction newTx1 = (HBaseTransaction) tm.begin();
        Get newGet1 = new Get(rowId);
        newGet1.addColumn(fam, qual);
        Result result = txTable.get(newTx1, newGet1);
        assertEquals(Bytes.toBytes("testValue 3"), result.getValue(fam, qual));
        // Write a new value
        Put newPut1 = new Put(rowId);
        newPut1.addColumn(fam, qual, Bytes.toBytes("new testValue 1"));
        txTable.put(newTx1, newPut1);

        // Start a second new transaction
        HBaseTransaction newTx2 = (HBaseTransaction) tm.begin();
        // Commit first of the new tx
        tm.commit(newTx1);

        // The second transaction should still read the previous value
        Get newGet2 = new Get(rowId);
        newGet2.addColumn(fam, qual);
        result = txTable.get(newTx2, newGet2);
        assertEquals(Bytes.toBytes("testValue 3"), result.getValue(fam, qual));
        tm.commit(newTx2);

        // Only two values -the new written by newTx1 and the last value
        // for the cell after compaction- should have survived
        assertFalse(CellUtils.hasCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                    "Put cell of Tx1 should not be there");
        assertFalse(CellUtils.hasShadowCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                    "Put shadow cell of Tx1 should not be there");
        assertFalse(CellUtils.hasCell(rowId, fam, qual, tx2.getStartTimestamp(), getter),
                    "Put cell of Tx2 should not be there");
        assertFalse(CellUtils.hasShadowCell(rowId, fam, qual, tx2.getStartTimestamp(), getter),
                    "Put shadow cell of Tx2 should not be there");
        assertTrue(CellUtils.hasCell(rowId, fam, qual, tx3.getStartTimestamp(), getter),
                   "Put cell of Tx3 cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, tx3.getStartTimestamp(), getter),
                   "Put shadow cell of Tx3 should be there");
        assertTrue(CellUtils.hasCell(rowId, fam, qual, newTx1.getStartTimestamp(), getter),
                   "Put cell of NewTx1 cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, newTx1.getStartTimestamp(), getter),
                   "Put shadow cell of NewTx1 should be there");
    }

    /**
     * Tests a case where a temporary failure to flush causes the compactor to crash
     */
    @Test(timeOut = 60_000)
    public void testDuplicateDeletes() throws Throwable {
        String TEST_TABLE = "testDuplicateDeletes";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(connection, TEST_TABLE);

        // jump through hoops to trigger a minor compaction.
        // a minor compaction will only run if there are enough
        // files to be compacted, but that is less than the number
        // of total files, in which case it will run a major
        // compaction. The issue this is testing only shows up
        // with minor compaction, as only Deletes can be duplicate
        // and major compactions filter them out.
        byte[] firstRow = "FirstRow".getBytes();
        HBaseTransaction tx0 = (HBaseTransaction) tm.begin();
        Put put0 = new Put(firstRow);
        put0.addColumn(fam, qual, Bytes.toBytes("testWrite-1"));
        txTable.put(tx0, put0);
        tm.commit(tx0);

        // create the first hfile
        manualFlush(TEST_TABLE);

        // write a row, it won't be committed
        byte[] rowToBeCompactedAway = "compactMe".getBytes();
        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        Put put1 = new Put(rowToBeCompactedAway);
        put1.addColumn(fam, qual, Bytes.toBytes("testWrite-1"));
        txTable.put(tx1, put1);
        txTable.flushCommits();

        // write a row to trigger the double delete problem
        byte[] row = "iCauseErrors".getBytes();
        HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
        Put put2 = new Put(row);
        put2.addColumn(fam, qual, Bytes.toBytes("testWrite-1"));
        txTable.put(tx2, put2);
        tm.commit(tx2);

        HBaseTransaction tx3 = (HBaseTransaction) tm.begin();
        Put put3 = new Put(row);
        put3.addColumn(fam, qual, Bytes.toBytes("testWrite-1"));
        txTable.put(tx3, put3);
        txTable.flushCommits();

        // cause a failure on HBaseTM#preCommit();
        Set<HBaseCellId> writeSet = tx3.getWriteSet();
        assertEquals(1, writeSet.size());
        List<HBaseCellId> newWriteSet = new ArrayList<>();
        final AtomicBoolean flushFailing = new AtomicBoolean(true);
        for (HBaseCellId id : writeSet) {
            TTable failableHTable = spy(id.getTable());
            doAnswer(new Answer<Void>() {
                @Override
                public Void answer(InvocationOnMock invocation)
                        throws Throwable {
                    if (flushFailing.get()) {
                        throw new RetriesExhaustedWithDetailsException(new ArrayList<Throwable>(),
                                                                       new ArrayList<Row>(), new ArrayList<String>());
                    } else {
                        invocation.callRealMethod();
                    }
                    return null;
                }
            }).when(failableHTable).flushCommits();

            newWriteSet.add(new HBaseCellId(failableHTable,
                                            id.getRow(), id.getFamily(),
                                            id.getQualifier(), id.getTimestamp()));
        }
        writeSet.clear();
        writeSet.addAll(newWriteSet);

        try {
            tm.commit(tx3);
            fail("Shouldn't succeed");
        } catch (TransactionException tme) {
            flushFailing.set(false);
            tm.rollback(tx3);
        }

        // create second hfile,
        // it should contain multiple deletes
        manualFlush(TEST_TABLE);

        // create loads of files
        byte[] anotherRow = "someotherrow".getBytes();
        HBaseTransaction tx4 = (HBaseTransaction) tm.begin();
        Put put4 = new Put(anotherRow);
        put4.addColumn(fam, qual, Bytes.toBytes("testWrite-1"));
        txTable.put(tx4, put4);
        tm.commit(tx4);

        // create third hfile
        manualFlush(TEST_TABLE);

        // trigger minor compaction and give it time to run
        setCompactorLWM(tx4.getStartTimestamp(), TEST_TABLE);
        admin.compact(TableName.valueOf(TEST_TABLE));
        Thread.sleep(3000);

        // check if the cell that should be compacted, is compacted
        assertFalse(CellUtils.hasCell(rowToBeCompactedAway, fam, qual, tx1.getStartTimestamp(),
                                      new TTableCellGetterAdapter(txTable)),
                    "Cell should not be be there");
    }

    @Test(timeOut = 60_000)
    public void testNonOmidCFIsUntouched() throws Throwable {
        String TEST_TABLE = "testNonOmidCFIsUntouched";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(connection, TEST_TABLE);

        admin.disableTable(TableName.valueOf(TEST_TABLE));
        byte[] nonOmidCF = Bytes.toBytes("nonOmidCF");
        byte[] nonOmidQual = Bytes.toBytes("nonOmidCol");
        HColumnDescriptor nonomidfam = new HColumnDescriptor(nonOmidCF);
        nonomidfam.setMaxVersions(MAX_VERSIONS);
        admin.addColumn(TableName.valueOf(TEST_TABLE), nonomidfam);
        admin.enableTable(TableName.valueOf(TEST_TABLE));

        byte[] rowId = Bytes.toBytes("testRow");
        Transaction tx = tm.begin();
        Put put = new Put(rowId);
        put.addColumn(fam, qual, Bytes.toBytes("testValue"));
        txTable.put(tx, put);

        Put nonTxPut = new Put(rowId);
        nonTxPut.addColumn(nonOmidCF, nonOmidQual, Bytes.toBytes("nonTxVal"));
        txTable.getHTable().put(nonTxPut);
        txTable.flushCommits(); // to make sure it left the client

        Get g = new Get(rowId);
        Result result = txTable.getHTable().get(g);
        assertEquals(result.getColumnCells(nonOmidCF, nonOmidQual).size(), 1, "Should be there, precompact");
        assertEquals(result.getColumnCells(fam, qual).size(), 1, "Should be there, precompact");

        compactEverything(TEST_TABLE);

        result = txTable.getHTable().get(g);
        assertEquals(result.getColumnCells(nonOmidCF, nonOmidQual).size(), 1, "Should be there, postcompact");
        assertEquals(result.getColumnCells(fam, qual).size(), 0, "Should not be there, postcompact");
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Tests on tombstones and non-transactional Deletes
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * Test that when a major compaction runs, cells that were deleted non-transactionally dissapear
     */
    @Test(timeOut = 60_000)
    public void testACellDeletedNonTransactionallyDoesNotAppearWhenAMajorCompactionOccurs() throws Throwable {
        String TEST_TABLE = "testACellDeletedNonTransactionallyDoesNotAppearWhenAMajorCompactionOccurs";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(connection, TEST_TABLE);

        Table table = txTable.getHTable();

        // Write first a value transactionally
        HBaseTransaction tx0 = (HBaseTransaction) tm.begin();
        byte[] rowId = Bytes.toBytes("row1");
        Put p0 = new Put(rowId);
        p0.addColumn(fam, qual, Bytes.toBytes("testValue-0"));
        txTable.put(tx0, p0);
        tm.commit(tx0);

        // Then perform a non-transactional Delete
        Delete d = new Delete(rowId);
        d.addColumn(fam, qual);
        table.delete(d);

        // Trigger a major compaction
        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();
        compactWithLWM(lwmTx.getStartTimestamp(), TEST_TABLE);

        // Then perform a non-tx (raw) scan...
        Scan scan = new Scan();
        scan.setRaw(true);
        ResultScanner scannerResults = table.getScanner(scan);

        // ...and test the deleted cell is not there anymore
        assertNull(scannerResults.next(), "There should be no results in scan results");

        table.close();

    }

    /**
     * Test that when a minor compaction runs, cells that were deleted non-transactionally are preserved. This is to
     * allow users still access the cells when doing "improper" operations on a transactional table
     */
    @Test(timeOut = 60_000)
    public void testACellDeletedNonTransactionallyIsPreservedWhenMinorCompactionOccurs() throws Throwable {
        String TEST_TABLE = "testACellDeletedNonTransactionallyIsPreservedWhenMinorCompactionOccurs";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(connection, TEST_TABLE);

        Table table = txTable.getHTable();

        // Configure the environment to create a minor compaction

        // Write first a value transactionally
        HBaseTransaction tx0 = (HBaseTransaction) tm.begin();
        byte[] rowId = Bytes.toBytes("row1");
        Put p0 = new Put(rowId);
        p0.addColumn(fam, qual, Bytes.toBytes("testValue-0"));
        txTable.put(tx0, p0);
        tm.commit(tx0);

        // create the first hfile
        manualFlush(TEST_TABLE);

        // Write another value transactionally
        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        Put p1 = new Put(rowId);
        p1.addColumn(fam, qual, Bytes.toBytes("testValue-1"));
        txTable.put(tx1, p1);
        tm.commit(tx1);

        // create the second hfile
        manualFlush(TEST_TABLE);

        // Write yet another value transactionally
        HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
        Put p2 = new Put(rowId);
        p2.addColumn(fam, qual, Bytes.toBytes("testValue-2"));
        txTable.put(tx2, p2);
        tm.commit(tx2);

        // create a third hfile
        manualFlush(TEST_TABLE);

        // Then perform a non-transactional Delete
        Delete d = new Delete(rowId);
        d.addColumn(fam, qual);
        table.delete(d);

        // create the fourth hfile
        manualFlush(TEST_TABLE);

        // Trigger the minor compaction
        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();
        setCompactorLWM(lwmTx.getStartTimestamp(), TEST_TABLE);
        admin.compact(TableName.valueOf(TEST_TABLE));
        Thread.sleep(5000);

        // Then perform a non-tx (raw) scan...
        Scan scan = new Scan();
        scan.setRaw(true);
        ResultScanner scannerResults = table.getScanner(scan);

        // ...and test the deleted cell is still there
        int count = 0;
        Result scanResult;
        List<Cell> listOfCellsScanned = new ArrayList<>();
        while ((scanResult = scannerResults.next()) != null) {
            listOfCellsScanned = scanResult.listCells(); // equivalent to rawCells()
            count++;
        }
        assertEquals(count, 1, "There should be only one result in scan results");
        assertEquals(listOfCellsScanned.size(), 3, "There should be 3 cell entries in scan results (2 puts, 1 del)");
        boolean wasDeletedCellFound = false;
        int numberOfDeletedCellsFound = 0;
        for (Cell cell : listOfCellsScanned) {
            if (CellUtil.isDelete(cell)) {
                wasDeletedCellFound = true;
                numberOfDeletedCellsFound++;
            }
        }
        assertTrue(wasDeletedCellFound, "We should have found a non-transactionally deleted cell");
        assertEquals(numberOfDeletedCellsFound, 1, "There should be only only one deleted cell");

        table.close();
    }

    /**
     * Test that when a minor compaction runs, tombstones are not cleaned up
     */
    @Test(timeOut = 60_000)
    public void testTombstonesAreNotCleanedUpWhenMinorCompactionOccurs() throws Throwable {
        String TEST_TABLE = "testTombstonesAreNotCleanedUpWhenMinorCompactionOccurs";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(connection, TEST_TABLE);

        // Configure the environment to create a minor compaction

        HBaseTransaction tx0 = (HBaseTransaction) tm.begin();
        byte[] rowId = Bytes.toBytes("case1");
        Put p = new Put(rowId);
        p.addColumn(fam, qual, Bytes.toBytes("testValue-0"));
        txTable.put(tx0, p);
        tm.commit(tx0);

        // create the first hfile
        manualFlush(TEST_TABLE);

        // Create the tombstone
        HBaseTransaction deleteTx = (HBaseTransaction) tm.begin();
        Delete d = new Delete(rowId);
        d.addColumn(fam, qual);
        txTable.delete(deleteTx, d);
        tm.commit(deleteTx);

        // create the second hfile
        manualFlush(TEST_TABLE);

        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        Put p1 = new Put(rowId);
        p1.addColumn(fam, qual, Bytes.toBytes("testValue-11"));
        txTable.put(tx1, p1);
        tm.commit(tx1);

        // create the third hfile
        manualFlush(TEST_TABLE);

        HBaseTransaction lastTx = (HBaseTransaction) tm.begin();
        Put p2 = new Put(rowId);
        p2.addColumn(fam, qual, Bytes.toBytes("testValue-222"));
        txTable.put(lastTx, p2);
        tm.commit(lastTx);

        // Trigger the minor compaction
        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();
        setCompactorLWM(lwmTx.getStartTimestamp(), TEST_TABLE);
        admin.compact(TableName.valueOf(TEST_TABLE));
        Thread.sleep(5000);

        // Checks on results after compaction
        TTableCellGetterAdapter getter = new TTableCellGetterAdapter(txTable);
        assertFalse(CellUtils.hasCell(rowId, fam, qual, tx0.getStartTimestamp(), getter), "Put cell should be there");
        assertFalse(CellUtils.hasShadowCell(rowId, fam, qual, tx0.getStartTimestamp(), getter),
                    "Put shadow cell should be there");
        assertTrue(CellUtils.hasCell(rowId, fam, qual, tx1.getStartTimestamp(), getter), "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                   "Put shadow cell should be there");
        assertTrue(CellUtils.hasCell(rowId, fam, qual, deleteTx.getStartTimestamp(), getter),
                   "Delete cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, deleteTx.getStartTimestamp(), getter),
                   "Delete shadow cell should be there");
        assertTrue(CellUtils.hasCell(rowId, fam, qual, lastTx.getStartTimestamp(), getter),
                   "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, lastTx.getStartTimestamp(), getter),
                   "Put shadow cell should be there");
    }


    /**
     * Test that when compaction runs, tombstones are cleaned up case1: 1 put (ts < lwm) then tombstone (ts > lwm)
     */
    @Test(timeOut = 60_000)
    public void testTombstonesAreCleanedUpCase1() throws Exception {
        String TEST_TABLE = "testTombstonesAreCleanedUpCase1";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(connection, TEST_TABLE);

        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        byte[] rowId = Bytes.toBytes("case1");
        Put p = new Put(rowId);
        p.addColumn(fam, qual, Bytes.toBytes("testValue"));
        txTable.put(tx1, p);
        tm.commit(tx1);

        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();
        setCompactorLWM(lwmTx.getStartTimestamp(), TEST_TABLE);

        HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
        Delete d = new Delete(rowId);
        d.addColumn(fam, qual);
        txTable.delete(tx2, d);
        tm.commit(tx2);

        TTableCellGetterAdapter getter = new TTableCellGetterAdapter(txTable);
        assertTrue(CellUtils.hasCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                   "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                   "Put shadow cell should be there");
        assertTrue(CellUtils.hasCell(rowId, fam, qual, tx2.getStartTimestamp(), getter),
                   "Delete cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, tx2.getStartTimestamp(), getter),
                   "Delete shadow cell should be there");
    }

    /**
     * Test that when compaction runs, tombstones are cleaned up case2: 1 put (ts < lwm) then tombstone (ts < lwm)
     */
    @Test(timeOut = 60_000)
    public void testTombstonesAreCleanedUpCase2() throws Exception {
        String TEST_TABLE = "testTombstonesAreCleanedUpCase2";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(connection, TEST_TABLE);

        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        byte[] rowId = Bytes.toBytes("case2");
        Put p = new Put(rowId);
        p.addColumn(fam, qual, Bytes.toBytes("testValue"));
        txTable.put(tx1, p);
        tm.commit(tx1);

        HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
        Delete d = new Delete(rowId);
        d.addColumn(fam, qual);
        txTable.delete(tx2, d);
        tm.commit(tx2);

        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();
        compactWithLWM(lwmTx.getStartTimestamp(), TEST_TABLE);

        TTableCellGetterAdapter getter = new TTableCellGetterAdapter(txTable);
        assertFalse(CellUtils.hasCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                    "Put cell shouldn't be there");
        assertFalse(CellUtils.hasShadowCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                    "Put shadow cell shouldn't be there");
        assertFalse(CellUtils.hasCell(rowId, fam, qual, tx2.getStartTimestamp(), getter),
                    "Delete cell shouldn't be there");
        assertFalse(CellUtils.hasShadowCell(rowId, fam, qual, tx2.getStartTimestamp(), getter),
                    "Delete shadow cell shouldn't be there");
    }


    //Cell level conflict detection
    @Test(timeOut = 60_000)
    public void testFamiliyDeleteTombstonesAreCleanedUpCellCF() throws Exception {
        String TEST_TABLE = "testFamiliyDeleteTombstonesAreCleanedUpCellCF";
        byte[] fam2 = Bytes.toBytes("2");
        byte[] fam3 = Bytes.toBytes("3");
        byte[] fam4 = Bytes.toBytes("4");
        createTableIfNotExists(TEST_TABLE, fam2, fam3, fam4);
        TTable txTable = new TTable(connection, TEST_TABLE);

        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        byte[] rowId = Bytes.toBytes("case2");
        byte[] qual2 = Bytes.toBytes("qual2");

        Put p = new Put(rowId);
        p.addColumn(fam2, qual, Bytes.toBytes("testValue"));
        p.addColumn(fam2, qual2 , Bytes.toBytes("testValue"));

        p.addColumn(fam3, qual, Bytes.toBytes("testValue"));
        p.addColumn(fam3, qual2 , Bytes.toBytes("testValue"));

        p.addColumn(fam4, qual, Bytes.toBytes("testValue"));
        p.addColumn(fam4, qual2 , Bytes.toBytes("testValue"));


        txTable.put(tx1, p);

        byte[] rowId2 = Bytes.toBytes("case22");
        Put p2 = new Put(rowId2);
        p2.addColumn(fam3, qual, Bytes.toBytes("testValue2"));
        txTable.put(tx1, p2);

        tm.commit(tx1);

        HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
        Delete d = new Delete(rowId);
        d.addFamily(fam3);
        txTable.delete(tx2, d);
        tm.commit(tx2);

        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();

        TTableCellGetterAdapter getter = new TTableCellGetterAdapter(txTable);
        assertTrue(CellUtils.hasCell(rowId, fam2, qual, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam2, qual, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");
        assertTrue(CellUtils.hasCell(rowId, fam2, qual2, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam2, qual2, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");

        assertTrue(CellUtils.hasCell(rowId, fam3, qual, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam3, qual, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");
        assertTrue(CellUtils.hasCell(rowId, fam3, qual, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam3, qual2, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");

        assertTrue(CellUtils.hasCell(rowId, fam4, qual, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam4, qual, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");
        assertTrue(CellUtils.hasCell(rowId, fam4, qual, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam4, qual2, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");


        assertTrue(CellUtils.hasCell(rowId, fam3, CellUtils.FAMILY_DELETE_QUALIFIER, tx2.getStartTimestamp(), getter),
                "Delete cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam3, CellUtils.FAMILY_DELETE_QUALIFIER, tx2.getStartTimestamp(), getter),
                "Delete shadow cell should be there");

        //Do major compaction
        compactWithLWM(lwmTx.getStartTimestamp(), TEST_TABLE);



        assertTrue(CellUtils.hasCell(rowId, fam2, qual, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam2, qual, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");
        assertTrue(CellUtils.hasCell(rowId, fam2, qual2, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam2, qual2, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");


        assertFalse(CellUtils.hasCell(rowId, fam3, qual, tx1.getStartTimestamp(), getter),
                "Put cell shouldn't be there");
        assertFalse(CellUtils.hasShadowCell(rowId, fam3, qual, tx1.getStartTimestamp(), getter),
                "Put shadow cell shouldn't be there");
        assertFalse(CellUtils.hasCell(rowId, fam3, qual2, tx1.getStartTimestamp(), getter),
                "Put cell shouldn't be there");
        assertFalse(CellUtils.hasShadowCell(rowId, fam3, qual2, tx1.getStartTimestamp(), getter),
                "Put shadow cell shouldn't be there");


        assertTrue(CellUtils.hasCell(rowId, fam4, qual, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam4, qual, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");
        assertTrue(CellUtils.hasCell(rowId, fam4, qual, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam4, qual2, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");



        assertFalse(CellUtils.hasCell(rowId, fam3, CellUtils.FAMILY_DELETE_QUALIFIER, tx2.getStartTimestamp(), getter),
                "Delete cell shouldn't be there");
        assertFalse(CellUtils.hasShadowCell(rowId, fam3, CellUtils.FAMILY_DELETE_QUALIFIER, tx2.getStartTimestamp(), getter),
                "Delete shadow cell shouldn't be there");
    }


    //Row level conflict detection
    @Test(timeOut = 60_000)
    public void testFamiliyDeleteTombstonesAreCleanedUpRowCF() throws Exception {
        String TEST_TABLE = "testFamiliyDeleteTombstonesAreCleanedUpRowCF";
        ((HBaseTransactionManager) tm).setConflictDetectionLevel(OmidClientConfiguration.ConflictDetectionLevel.ROW);

        byte[] fam2 = Bytes.toBytes("2");
        byte[] fam3 = Bytes.toBytes("3");
        byte[] fam4 = Bytes.toBytes("4");
        createTableIfNotExists(TEST_TABLE, fam2, fam3, fam4);
        TTable txTable = new TTable(connection, TEST_TABLE);

        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        byte[] rowId = Bytes.toBytes("case2");
        byte[] qual2 = Bytes.toBytes("qual2");

        Put p = new Put(rowId);
        p.addColumn(fam2, qual, Bytes.toBytes("testValue"));
        p.addColumn(fam2, qual2 , Bytes.toBytes("testValue"));

        p.addColumn(fam3, qual, Bytes.toBytes("testValue"));
        p.addColumn(fam3, qual2 , Bytes.toBytes("testValue"));

        p.addColumn(fam4, qual, Bytes.toBytes("testValue"));
        p.addColumn(fam4, qual2 , Bytes.toBytes("testValue"));


        txTable.put(tx1, p);

        byte[] rowId2 = Bytes.toBytes("case22");
        Put p2 = new Put(rowId2);
        p2.addColumn(fam3, qual, Bytes.toBytes("testValue2"));
        txTable.put(tx1, p2);

        tm.commit(tx1);

        HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
        Delete d = new Delete(rowId);
        d.addFamily(fam3);
        txTable.delete(tx2, d);
        tm.commit(tx2);

        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();

        TTableCellGetterAdapter getter = new TTableCellGetterAdapter(txTable);
        assertTrue(CellUtils.hasCell(rowId, fam2, qual, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam2, qual, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");
        assertTrue(CellUtils.hasCell(rowId, fam2, qual2, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam2, qual2, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");

        assertTrue(CellUtils.hasCell(rowId, fam3, qual, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam3, qual, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");
        assertTrue(CellUtils.hasCell(rowId, fam3, qual, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam3, qual2, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");

        assertTrue(CellUtils.hasCell(rowId, fam4, qual, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam4, qual, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");
        assertTrue(CellUtils.hasCell(rowId, fam4, qual, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam4, qual2, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");


        assertTrue(CellUtils.hasCell(rowId, fam3, CellUtils.FAMILY_DELETE_QUALIFIER, tx2.getStartTimestamp(), getter),
                "Delete cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam3, CellUtils.FAMILY_DELETE_QUALIFIER, tx2.getStartTimestamp(), getter),
                "Delete shadow cell should be there");

        //Do major compaction
        compactWithLWM(lwmTx.getStartTimestamp(), TEST_TABLE);



        assertTrue(CellUtils.hasCell(rowId, fam2, qual, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam2, qual, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");
        assertTrue(CellUtils.hasCell(rowId, fam2, qual2, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam2, qual2, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");


        assertFalse(CellUtils.hasCell(rowId, fam3, qual, tx1.getStartTimestamp(), getter),
                "Put cell shouldn't be there");
        assertFalse(CellUtils.hasShadowCell(rowId, fam3, qual, tx1.getStartTimestamp(), getter),
                "Put shadow cell shouldn't be there");
        assertFalse(CellUtils.hasCell(rowId, fam3, qual2, tx1.getStartTimestamp(), getter),
                "Put cell shouldn't be there");
        assertFalse(CellUtils.hasShadowCell(rowId, fam3, qual2, tx1.getStartTimestamp(), getter),
                "Put shadow cell shouldn't be there");


        assertTrue(CellUtils.hasCell(rowId, fam4, qual, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam4, qual, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");
        assertTrue(CellUtils.hasCell(rowId, fam4, qual, tx1.getStartTimestamp(), getter),
                "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam4, qual2, tx1.getStartTimestamp(), getter),
                "Put shadow cell should be there");



        assertFalse(CellUtils.hasCell(rowId, fam3, CellUtils.FAMILY_DELETE_QUALIFIER, tx2.getStartTimestamp(), getter),
                "Delete cell shouldn't be there");
        assertFalse(CellUtils.hasShadowCell(rowId, fam3, CellUtils.FAMILY_DELETE_QUALIFIER, tx2.getStartTimestamp(), getter),
                "Delete shadow cell shouldn't be there");
    }



    /**
     * Test that when compaction runs, tombstones are cleaned up case3: 1 put (ts < lwm) then tombstone (ts < lwm) not
     * committed
     */
    @Test(timeOut = 60_000)
    public void testTombstonesAreCleanedUpCase3() throws Exception {
        String TEST_TABLE = "testTombstonesAreCleanedUpCase3";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(connection, TEST_TABLE);

        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        byte[] rowId = Bytes.toBytes("case3");
        Put p = new Put(rowId);
        p.addColumn(fam, qual, Bytes.toBytes("testValue"));
        txTable.put(tx1, p);
        tm.commit(tx1);

        HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
        Delete d = new Delete(rowId);
        d.addColumn(fam, qual);
        txTable.delete(tx2, d);

        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();
        compactWithLWM(lwmTx.getStartTimestamp(), TEST_TABLE);

        TTableCellGetterAdapter getter = new TTableCellGetterAdapter(txTable);
        assertTrue(CellUtils.hasCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                   "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                   "Put shadow cell shouldn't be there");
        assertFalse(CellUtils.hasCell(rowId, fam, qual, tx2.getStartTimestamp(), getter),
                    "Delete cell shouldn't be there");
        assertFalse(CellUtils.hasShadowCell(rowId, fam, qual, tx2.getStartTimestamp(), getter),
                    "Delete shadow cell shouldn't be there");
    }

    /**
     * Test that when compaction runs, tombstones are cleaned up case4: 1 put (ts < lwm) then tombstone (ts > lwm) not
     * committed
     */
    @Test(timeOut = 60_000)
    public void testTombstonesAreCleanedUpCase4() throws Exception {
        String TEST_TABLE = "testTombstonesAreCleanedUpCase4";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(connection, TEST_TABLE);

        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        byte[] rowId = Bytes.toBytes("case4");
        Put p = new Put(rowId);
        p.addColumn(fam, qual, Bytes.toBytes("testValue"));
        txTable.put(tx1, p);
        tm.commit(tx1);

        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();

        HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
        Delete d = new Delete(rowId);
        d.addColumn(fam, qual);
        txTable.delete(tx2, d);
        compactWithLWM(lwmTx.getStartTimestamp(), TEST_TABLE);

        TTableCellGetterAdapter getter = new TTableCellGetterAdapter(txTable);
        assertTrue(CellUtils.hasCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                   "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                   "Put shadow cell shouldn't be there");
        assertTrue(CellUtils.hasCell(rowId, fam, qual,tx2.getStartTimestamp(), getter),
                   "Delete cell should be there");
        assertFalse(CellUtils.hasShadowCell(rowId, fam, qual, tx2.getStartTimestamp(), getter),
                    "Delete shadow cell shouldn't be there");
    }

    /**
     * Test that when compaction runs, tombstones are cleaned up case5: tombstone (ts < lwm)
     */
    @Test(timeOut = 60_000)
    public void testTombstonesAreCleanedUpCase5() throws Exception {
        String TEST_TABLE = "testTombstonesAreCleanedUpCase5";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(connection, TEST_TABLE);

        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        byte[] rowId = Bytes.toBytes("case5");
        Delete d = new Delete(rowId);
        d.addColumn(fam, qual);
        txTable.delete(tx1, d);
        tm.commit(tx1);

        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();
        compactWithLWM(lwmTx.getStartTimestamp(), TEST_TABLE);

        TTableCellGetterAdapter getter = new TTableCellGetterAdapter(txTable);
        assertFalse(CellUtils.hasCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                    "Delete cell shouldn't be there");
        assertFalse(CellUtils.hasShadowCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                    "Delete shadow cell shouldn't be there");
    }

    /**
     * Test that when compaction runs, tombstones are cleaned up case6: tombstone (ts < lwm), then put (ts < lwm)
     */
    @Test(timeOut = 60_000)
    public void testTombstonesAreCleanedUpCase6() throws Exception {
        String TEST_TABLE = "testTombstonesAreCleanedUpCase6";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(connection, TEST_TABLE);
        byte[] rowId = Bytes.toBytes("case6");

        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        Delete d = new Delete(rowId);
        d.addColumn(fam, qual);
        txTable.delete(tx1, d);
        tm.commit(tx1);

        HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
        Put p = new Put(rowId);
        p.addColumn(fam, qual, Bytes.toBytes("testValue"));
        txTable.put(tx2, p);
        tm.commit(tx2);

        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();
        compactWithLWM(lwmTx.getStartTimestamp(), TEST_TABLE);

        TTableCellGetterAdapter getter = new TTableCellGetterAdapter(txTable);
        assertFalse(CellUtils.hasCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                    "Delete cell shouldn't be there");
        assertFalse(CellUtils.hasShadowCell(rowId, fam, qual, tx1.getStartTimestamp(), getter),
                    "Delete shadow cell shouldn't be there");
        assertTrue(CellUtils.hasCell(rowId, fam, qual, tx2.getStartTimestamp(), getter),
                   "Put cell should be there");
        assertTrue(CellUtils.hasShadowCell(rowId, fam, qual, tx2.getStartTimestamp(), getter),
                   "Put shadow cell shouldn't be there");
    }

    @Test(timeOut = 60_000)
    public void testCommitTableNoInvalidation() throws Exception {
        String TEST_TABLE = "testCommitTableInvalidation";
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        TTable txTable = new TTable(connection, TEST_TABLE);
        byte[] rowId = Bytes.toBytes("row");

        HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
        Put p = new Put(rowId);
        p.addColumn(fam, qual, Bytes.toBytes("testValue"));
        txTable.put(tx1, p);

        HBaseTransaction lwmTx = (HBaseTransaction) tm.begin();
        compactWithLWM(lwmTx.getStartTimestamp(), TEST_TABLE);

        try {
            //give compaction time to invalidate
            Thread.sleep(1000);

            tm.commit(tx1);

        } catch (RollbackException e) {
            fail(" Should have not been invalidated");
        }
    }


    private void setCompactorLWM(long lwm, String tableName) throws Exception {
        OmidCompactor omidCompactor = (OmidCompactor) hbaseCluster.getRegions(Bytes.toBytes(tableName)).get(0)
                .getCoprocessorHost().findCoprocessor(OmidCompactor.class.getName());
        CommitTable commitTable = injector.getInstance(CommitTable.class);
        CommitTable.Client commitTableClient = spy(commitTable.getClient());
        SettableFuture<Long> f = SettableFuture.create();
        f.set(lwm);
        doReturn(f).when(commitTableClient).readLowWatermark();
        omidCompactor.commitTableClient = commitTableClient;
    }

    private void compactEverything(String tableName) throws Exception {
        compactWithLWM(Long.MAX_VALUE, tableName);
    }

    private void compactWithLWM(long lwm, String tableName) throws Exception {
        admin.flush(TableName.valueOf(tableName));

        LOG.info("Regions in table {}: {}", tableName, hbaseCluster.getRegions(Bytes.toBytes(tableName)).size());
        setCompactorLWM(lwm, tableName);
        LOG.info("Compacting table {}", tableName);
        admin.majorCompact(TableName.valueOf(tableName));

        LOG.info("Sleeping for 3 secs");
        Thread.sleep(3000);
        LOG.info("Waking up after 3 secs");
    }

    private static long rowCount(String tableName, byte[] family) throws Throwable {
        Scan scan = new Scan();
        scan.addFamily(family);
        Table table = connection.getTable(TableName.valueOf(tableName));
        try (ResultScanner scanner = table.getScanner(scan)) {
            int count = 0;
            while (scanner.next() != null) {
                count++;
            }
            return count;
        }
    }
}
