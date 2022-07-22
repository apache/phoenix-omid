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


import static org.mockito.Mockito.doReturn;

import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
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

import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestCompactionLL {

    private static final Logger LOG = LoggerFactory.getLogger(TestCompactionLL.class);

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
        tsoConfig.setPort(1235);
        tsoConfig.setConflictMapSize(1);
        tsoConfig.setLowLatency(true);
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

            ArrayList<ColumnFamilyDescriptor> fams = new ArrayList<>();
            for (byte[] family : families) {
                fams.add(ColumnFamilyDescriptorBuilder
                    .newBuilder(family)
                    .setMaxVersions(MAX_VERSIONS)
                    .build());
            }
            TableDescriptor desc = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf(tableName))
                    .setColumnFamilies(fams)
                    .setCoprocessor(CoprocessorDescriptorBuilder
                            .newBuilder("org.apache.hadoop.hbase.coprocessor.AggregateImplementation")
                            .setPriority(Coprocessor.PRIORITY_HIGHEST).build())
                    .build();
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
        TestUtils.waitForSocketListening("localhost", 1235, 100);
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
        TestUtils.waitForSocketNotListening("localhost", 1235, 1000);
    }

    @BeforeMethod
    public void setupTestCompactionIndividualTest() throws Exception {
        randomGenerator = new Random(0xfeedcafeL);
        tm = spy((AbstractTransactionManager) newTransactionManager());
    }

    private TransactionManager newTransactionManager() throws Exception {
        HBaseOmidClientConfiguration hbaseOmidClientConf = new HBaseOmidClientConfiguration();
        hbaseOmidClientConf.setConnectionString("localhost:1235");
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
    public void testRowsUnalteredWhenCommitTableCannotBeReached() throws Throwable {
        String TEST_TABLE = "testRowsUnalteredWhenCommitTableCannotBeReachedLL";
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
    // test omid-147 in ll mode the scanner should invalidate the transaction
    public void testCommitTableInvalidation() throws Exception {
        String TEST_TABLE = "testCommitTableInvalidationLL";
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
            fail(" Should have been invalidated");
        } catch (RollbackException e) {
            e.printStackTrace();
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
