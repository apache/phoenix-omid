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
package org.apache.omid.committable.hbase;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.CommitTable.Client;
import org.apache.omid.committable.CommitTable.CommitTimestamp;
import org.apache.omid.committable.CommitTable.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.apache.phoenix.thirdparty.com.google.common.base.Optional;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ListenableFuture;

public class TestHBaseCommitTable {

    private static final Logger LOG = LoggerFactory.getLogger(TestHBaseCommitTable.class);

    private static final String TEST_TABLE = "TEST";

    private static final TableName TABLE_NAME = TableName.valueOf(TEST_TABLE);

    private static HBaseTestingUtility testutil;
    private static MiniHBaseCluster hbasecluster;
    protected static Configuration hbaseConf;
    protected static Connection connection;
    private byte[] commitTableFamily;
    private byte[] lowWatermarkFamily;


    @BeforeClass
    public void setUpClass() throws Exception {
        // HBase setup
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.setBoolean("hbase.localcluster.assign.random.ports",true);
        DefaultHBaseCommitTableStorageModule module = new DefaultHBaseCommitTableStorageModule();
        commitTableFamily = module.getFamilyName().getBytes();
        lowWatermarkFamily = module.getLowWatermarkFamily().getBytes();
        LOG.info("Create hbase");
        testutil = new HBaseTestingUtility(hbaseConf);
        hbasecluster = testutil.startMiniCluster(1);
        connection = ConnectionFactory.createConnection(hbaseConf);
    }

    @AfterClass
    public void tearDownClass() throws Exception {
        if (hbasecluster != null) {
            testutil.shutdownMiniCluster();
        }
    }

    @BeforeMethod
    public void setUp() throws Exception {
        Admin admin = testutil.getAdmin();

        if (!admin.tableExists(TableName.valueOf(TEST_TABLE))) {

            ArrayList<ColumnFamilyDescriptor> fams = new ArrayList<>();
            fams.add(ColumnFamilyDescriptorBuilder
                    .newBuilder(commitTableFamily)
                    .setMaxVersions(Integer.MAX_VALUE)
                    .build());
            fams.add(ColumnFamilyDescriptorBuilder
                .newBuilder(lowWatermarkFamily)
                .setMaxVersions(Integer.MAX_VALUE)
                .build());

            TableDescriptor desc = TableDescriptorBuilder
                    .newBuilder(TABLE_NAME)
                    .setColumnFamilies(fams)
                    .build();
            admin.createTable(desc);
        }

        if (admin.isTableDisabled(TableName.valueOf(TEST_TABLE))) {
            admin.enableTable(TableName.valueOf(TEST_TABLE));
        }
        TableDescriptor[] tables = admin.listTables();
        for (TableDescriptor t : tables) {
            LOG.info(t.getTableName().getNameAsString());
        }
    }

    @AfterMethod
    public void tearDown() {
        try {
            LOG.info("tearing Down");
            Admin admin = testutil.getAdmin();
            admin.disableTable(TableName.valueOf(TEST_TABLE));
            admin.deleteTable(TableName.valueOf(TEST_TABLE));

        } catch (Exception e) {
            LOG.error("Error tearing down", e);
        }
    }

    @Test(timeOut = 30_000)
    public void testBasicBehaviour() throws Throwable {
        HBaseCommitTableConfig config = new HBaseCommitTableConfig();
        config.setTableName(TEST_TABLE);
        HBaseCommitTable commitTable = new HBaseCommitTable(connection, config);

        Writer writer = commitTable.getWriter();
        Client client = commitTable.getClient();

        // Test that the first time the table is empty
        assertEquals(rowCount(TABLE_NAME, commitTableFamily), 0, "Rows should be 0!");

        // Test the successful creation of 1000 txs in the table
        for (int i = 0; i < 1000; i+=CommitTable.MAX_CHECKPOINTS_PER_TXN) {
            writer.addCommittedTransaction(i, i + 1);
        }
        writer.flush();
        assertEquals(rowCount(TABLE_NAME, commitTableFamily), 1000/CommitTable.MAX_CHECKPOINTS_PER_TXN, "Rows should be 1000!");

        // Test the we get the right commit timestamps for each previously inserted tx
        for (long i = 0; i < 1000; i++) {
            Optional<CommitTimestamp> commitTimestamp = client.getCommitTimestamp(i).get();
            assertTrue(commitTimestamp.isPresent());
            assertTrue(commitTimestamp.get().isValid());
            long ct = commitTimestamp.get().getValue();
            long expected = i - (i % CommitTable.MAX_CHECKPOINTS_PER_TXN) + 1;
            assertEquals(ct, expected, "Commit timestamp should be " + expected);
        }
        assertEquals(rowCount(TABLE_NAME, commitTableFamily), 1000/CommitTable.MAX_CHECKPOINTS_PER_TXN, "Rows should be 1000!");

        // Test the successful deletion of the 1000 txs
        Future<Void> f;
        for (long i = 0; i < 1000; i+=CommitTable.MAX_CHECKPOINTS_PER_TXN) {
            f = client.deleteCommitEntry(i);
            f.get();
        }
        assertEquals(rowCount(TABLE_NAME, commitTableFamily), 0, "Rows should be 0!");

        // Test we don't get a commit timestamp for a non-existent transaction id in the table
        Optional<CommitTimestamp> commitTimestamp = client.getCommitTimestamp(0).get();
        assertFalse(commitTimestamp.isPresent(), "Commit timestamp should not be present");

        // Test that the first time, the low watermark family in table is empty
        assertEquals(rowCount(TABLE_NAME, lowWatermarkFamily), 0, "Rows should be 0!");

        // Test the unsuccessful read of the low watermark the first time
        ListenableFuture<Long> lowWatermarkFuture = client.readLowWatermark();
        assertEquals(lowWatermarkFuture.get(), Long.valueOf(0), "Low watermark should be 0");

        // Test the successful update of the low watermark
        for (int lowWatermark = 0; lowWatermark < 1000; lowWatermark++) {
            writer.updateLowWatermark(lowWatermark);
        }
        writer.flush();
        assertEquals(rowCount(TABLE_NAME, lowWatermarkFamily), 1, "Should there be only one row!");

        // Test the successful read of the low watermark
        lowWatermarkFuture = client.readLowWatermark();
        long lowWatermark = lowWatermarkFuture.get();
        assertEquals(lowWatermark, 999, "Low watermark should be 999");
        assertEquals(rowCount(TABLE_NAME, lowWatermarkFamily), 1, "Should there be only one row!");

    }


    @Test(timeOut = 30_000)
    public void testCheckpoints() throws Throwable {
        HBaseCommitTableConfig config = new HBaseCommitTableConfig();
        config.setTableName(TEST_TABLE);
        HBaseCommitTable commitTable = new HBaseCommitTable(connection, config);

        Writer writer = commitTable.getWriter();
        Client client = commitTable.getClient();

        // Test that the first time the table is empty
        assertEquals(rowCount(TABLE_NAME, commitTableFamily), 0, "Rows should be 0!");

        long st = 0;
        long ct = 1;

        // Add a single commit that may represent many checkpoints
        writer.addCommittedTransaction(st, ct);
        writer.flush();

        for (int i=0; i<CommitTable.MAX_CHECKPOINTS_PER_TXN;++i) {
            Optional<CommitTimestamp> commitTimestamp = client.getCommitTimestamp(i).get();
            assertTrue(commitTimestamp.isPresent());
            assertTrue(commitTimestamp.get().isValid());
            assertEquals(ct, commitTimestamp.get().getValue());
        }

        // try invalidate based on start timestamp from a checkpoint
        assertFalse(client.tryInvalidateTransaction(st + 1).get());

        long st2 = 100;
        long ct2 = 101;

        // now invalidate a not committed transaction and then commit
        assertTrue(client.tryInvalidateTransaction(st2 + 1).get());
        assertFalse(writer.atomicAddCommittedTransaction(st2, ct2));

        //test delete
        client.deleteCommitEntry(st2 + 1).get();
        //now committing should work
        assertTrue(writer.atomicAddCommittedTransaction(st2, ct2));
    }



    @Test(timeOut = 30_000)
    public void testTransactionInvalidation() throws Throwable {

        // Prepare test
        final int TX1_ST = 0;
        final int TX1_CT = TX1_ST + 1;
        final int TX2_ST = 0 + CommitTable.MAX_CHECKPOINTS_PER_TXN;
        final int TX2_CT = TX2_ST + 1;

        HBaseCommitTableConfig config = new HBaseCommitTableConfig();
        config.setTableName(TEST_TABLE);
        HBaseCommitTable commitTable = new HBaseCommitTable(connection, config);

        // Components under test
        Writer writer = commitTable.getWriter();
        Client client = commitTable.getClient();

        // Test that initially the table is empty
        assertEquals(rowCount(TABLE_NAME, commitTableFamily), 0, "Rows should be 0!");

        // Test that a transaction can be added properly to the commit table
        writer.addCommittedTransaction(TX1_ST, TX1_CT);
        writer.flush();
        Optional<CommitTimestamp> commitTimestamp = client.getCommitTimestamp(TX1_ST).get();
        assertTrue(commitTimestamp.isPresent());
        assertTrue(commitTimestamp.get().isValid());
        long ct = commitTimestamp.get().getValue();
        assertEquals(ct, TX1_CT, "Commit timestamp should be " + TX1_CT);

        // Test that a committed transaction cannot be invalidated and
        // preserves its commit timestamp after that
        boolean wasInvalidated = client.tryInvalidateTransaction(TX1_ST).get();
        assertFalse(wasInvalidated, "Transaction should not be invalidated");

        commitTimestamp = client.getCommitTimestamp(TX1_ST).get();
        assertTrue(commitTimestamp.isPresent());
        assertTrue(commitTimestamp.get().isValid());
        ct = commitTimestamp.get().getValue();
        assertEquals(ct, TX1_CT, "Commit timestamp should be " + TX1_CT);

        // Test that a non-committed transaction can be invalidated...
        wasInvalidated = client.tryInvalidateTransaction(TX2_ST).get();
        assertTrue(wasInvalidated, "Transaction should be invalidated");
        commitTimestamp = client.getCommitTimestamp(TX2_ST).get();
        assertTrue(commitTimestamp.isPresent());
        assertFalse(commitTimestamp.get().isValid());
        ct = commitTimestamp.get().getValue();
        assertEquals(ct, CommitTable.INVALID_TRANSACTION_MARKER,
                     "Commit timestamp should be " + CommitTable.INVALID_TRANSACTION_MARKER);
        // ...and that if it has been already invalidated, it remains
        // invalidated when someone tries to commit it
        writer.addCommittedTransaction(TX2_ST, TX2_CT);
        writer.flush();
        commitTimestamp = client.getCommitTimestamp(TX2_ST).get();
        assertTrue(commitTimestamp.isPresent());
        assertFalse(commitTimestamp.get().isValid());
        ct = commitTimestamp.get().getValue();
        assertEquals(ct, CommitTable.INVALID_TRANSACTION_MARKER,
                     "Commit timestamp should be " + CommitTable.INVALID_TRANSACTION_MARKER);

        // Test that at the end of the test, the commit table contains 2
        // elements, which correspond to the two rows added in the test
        assertEquals(rowCount(TABLE_NAME, commitTableFamily), 2, "Rows should be 2!");

    }

    private static long rowCount(TableName tableName, byte[] family) throws Throwable {
        Scan scan = new Scan();
        scan.addFamily(family);
        Table table = connection.getTable(tableName);
        try (ResultScanner scanner = table.getScanner(scan)) {
            int count = 0;
            while (scanner.next() != null) {
                count++;
            }
            return count;
        }
    }

}
