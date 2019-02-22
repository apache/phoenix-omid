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

import static org.apache.omid.committable.CommitTable.CommitTimestamp.Location.CACHE;
import static org.apache.omid.committable.CommitTable.CommitTimestamp.Location.COMMIT_TABLE;
import static org.apache.omid.committable.CommitTable.CommitTimestamp.Location.NOT_PRESENT;
import static org.apache.omid.committable.CommitTable.CommitTimestamp.Location.SHADOW_CELL;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.CommitTable.CommitTimestamp;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.transaction.HBaseTransactionManager.CommitTimestampLocatorImpl;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SettableFuture;

@Test(groups = "sharedHBase")
public class TestHBaseTransactionClient extends OmidTestBase {

    private static final byte[] row1 = Bytes.toBytes("test-is-committed1");
    private static final byte[] row2 = Bytes.toBytes("test-is-committed2");
    private static final byte[] family = Bytes.toBytes(TEST_FAMILY);
    private static final byte[] qualifier = Bytes.toBytes("testdata");
    private static final byte[] data1 = Bytes.toBytes("testWrite-1");

    @Test(timeOut = 30_000)
    public void testIsCommitted(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        Table htable = connection.getTable(TableName.valueOf(TEST_TABLE));
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(new HTableAccessWrapper(htable, htable),
                ((AbstractTransactionManager)tm).getCommitTableClient());
        TTable table = spy(new TTable(htable, snapshotFilter, false));

        HBaseTransaction t1 = (HBaseTransaction) tm.begin();

        Put put = new Put(row1);
        put.addColumn(family, qualifier, data1);
        table.put(t1, put);
        tm.commit(t1);

        HBaseTransaction t2 = (HBaseTransaction) tm.begin();
        put = new Put(row2);
        put.addColumn(family, qualifier, data1);
        table.put(t2, put);
        table.flushCommits();

        HBaseTransaction t3 = (HBaseTransaction) tm.begin();
        put = new Put(row2);
        put.addColumn(family, qualifier, data1);
        table.put(t3, put);
        tm.commit(t3);

        HBaseCellId hBaseCellId1 = HBaseCellId.valueOf(t1, table, row1, family, qualifier, t1.getStartTimestamp());
        HBaseCellId hBaseCellId2 = HBaseCellId.valueOf(t2, table, row2, family, qualifier, t2.getStartTimestamp());
        HBaseCellId hBaseCellId3 = HBaseCellId.valueOf(t3, table, row2, family, qualifier, t3.getStartTimestamp());

        assertTrue(snapshotFilter.isCommitted(hBaseCellId1, 0, false), "row1 should be committed");
        assertFalse(snapshotFilter.isCommitted(hBaseCellId2, 0, false), "row2 should not be committed for kv2");
        assertTrue(snapshotFilter.isCommitted(hBaseCellId3, 0, false), "row2 should be committed for kv3");
    }

    @Test(timeOut = 30_000)
    public void testCrashAfterCommit(ITestContext context) throws Exception {
        PostCommitActions syncPostCommitter =
                spy(new HBaseSyncPostCommitter(new NullMetricsProvider(), getCommitTable(context).getClient()));
        AbstractTransactionManager tm = (AbstractTransactionManager) newTransactionManager(context, syncPostCommitter);
        // The following line emulates a crash after commit that is observed in (*) below
        doThrow(new RuntimeException()).when(syncPostCommitter).updateShadowCells(any(HBaseTransaction.class));

        Table htable = connection.getTable(TableName.valueOf(TEST_TABLE));
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(new HTableAccessWrapper(htable, htable),
                tm.getCommitTableClient());
        TTable table = spy(new TTable(htable, snapshotFilter, false));

        HBaseTransaction t1 = (HBaseTransaction) tm.begin();

        // Test shadow cell are created properly
        Put put = new Put(row1);
        put.addColumn(family, qualifier, data1);
        table.put(t1, put);
        try {
            tm.commit(t1);
        } catch (Exception e) { // (*) crash
            // Do nothing
        }

        assertTrue(CellUtils.hasCell(row1, family, qualifier, t1.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                   "Cell should be there");
        assertFalse(CellUtils.hasShadowCell(row1, family, qualifier, t1.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                    "Shadow cell should not be there");

        HBaseCellId hBaseCellId = HBaseCellId.valueOf(t1, table, row1, family, qualifier, t1.getStartTimestamp());

        HBaseTransactionClient hbaseTm = (HBaseTransactionClient) newTransactionManager(context);
        assertTrue(snapshotFilter.isCommitted(hBaseCellId, 0, false), "row1 should be committed");
    }

    @Test(timeOut = 30_000)
    public void testReadCommitTimestampFromCommitTable(ITestContext context) throws Exception {

        //connection = ConnectionFactory.createConnection(hbaseConf);
        final long NON_EXISTING_CELL_TS = 1000L;

        PostCommitActions syncPostCommitter =
                spy(new HBaseSyncPostCommitter(new NullMetricsProvider(), getCommitTable(context).getClient()));
        AbstractTransactionManager tm = (AbstractTransactionManager) newTransactionManager(context, syncPostCommitter);
        // The following line emulates a crash after commit that is observed in (*) below
        doThrow(new RuntimeException()).when(syncPostCommitter).updateShadowCells(any(HBaseTransaction.class));

        // Test that a non-existing cell timestamp returns an empty result
        Optional<CommitTimestamp> optionalCT = tm.commitTableClient.getCommitTimestamp(NON_EXISTING_CELL_TS).get();
        assertFalse(optionalCT.isPresent());

        try (TTable table = spy(new TTable(connection, TEST_TABLE, tm.getCommitTableClient()))) {
            // Test that we get an invalidation mark for an invalidated transaction

            // Start a transaction and invalidate it before commiting it
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            Put put = new Put(row1);
            put.addColumn(family, qualifier, data1);
            table.put(tx1, put);

            assertTrue(tm.commitTableClient.tryInvalidateTransaction(tx1.getStartTimestamp()).get());
            optionalCT = tm.commitTableClient.getCommitTimestamp(tx1.getStartTimestamp()).get();
            assertTrue(optionalCT.isPresent());
            CommitTimestamp ct = optionalCT.get();
            assertFalse(ct.isValid());
            assertEquals(ct.getValue(), CommitTable.INVALID_TRANSACTION_MARKER);
            assertTrue(ct.getLocation().compareTo(COMMIT_TABLE) == 0);

            // Finally test that we get the right commit timestamp for a committed tx
            // that couldn't get
            HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
            Put otherPut = new Put(row1);
            otherPut.addColumn(family, qualifier, data1);
            table.put(tx2, otherPut);
            try {
                tm.commit(tx2);
            } catch (Exception e) { // (*) crash
                // Do nothing
            }

            optionalCT = tm.commitTableClient.getCommitTimestamp(tx2.getStartTimestamp()).get();
            assertTrue(optionalCT.isPresent());
            ct = optionalCT.get();
            assertTrue(ct.isValid());
            assertEquals(ct.getValue(), tx2.getCommitTimestamp());
            assertTrue(ct.getLocation().compareTo(COMMIT_TABLE) == 0);
        }
    }

    @Test(timeOut = 30_000)
    public void testReadCommitTimestampFromShadowCell(ITestContext context) throws Exception {

        final long NON_EXISTING_CELL_TS = 1L;

        HBaseTransactionManager tm = (HBaseTransactionManager) newTransactionManager(context);

        Table htable = connection.getTable(TableName.valueOf(TEST_TABLE));
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(new HTableAccessWrapper(htable, htable),
                tm.getCommitTableClient());

        try (TTable table = spy(new TTable(htable, snapshotFilter, false))) {

            // Test first we can not found a non-existent cell ts
            HBaseCellId hBaseCellId = HBaseCellId.valueOf(tm.getConflictDetectionLevel(), table, row1, family, qualifier, NON_EXISTING_CELL_TS);
            // Set an empty cache to allow to bypass the checking
            CommitTimestampLocator ctLocator = new CommitTimestampLocatorImpl(hBaseCellId,
                    Maps.<Long, Long>newHashMap());
            Optional<CommitTimestamp> optionalCT = snapshotFilter
                    .readCommitTimestampFromShadowCell(NON_EXISTING_CELL_TS, ctLocator);
            assertFalse(optionalCT.isPresent());

            // Then test that for a transaction committed, we get the right CT
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            Put put = new Put(row1);
            put.addColumn(family, qualifier, data1);
            table.put(tx1, put);
            tm.commit(tx1);
            // Upon commit, the commit data should be in the shadow cells, so test it
            optionalCT = snapshotFilter.readCommitTimestampFromShadowCell(tx1.getStartTimestamp(), ctLocator);
            assertTrue(optionalCT.isPresent());
            CommitTimestamp ct = optionalCT.get();
            assertTrue(ct.isValid());
            assertEquals(ct.getValue(), tx1.getCommitTimestamp());
            assertTrue(ct.getLocation().compareTo(SHADOW_CELL) == 0);

        }

    }

    // Tests step 1 in AbstractTransactionManager.locateCellCommitTimestamp()
    @Test(timeOut = 30_000)
    public void testCellCommitTimestampIsLocatedInCache(ITestContext context) throws Exception {

        final long CELL_ST = 1L;
        final long CELL_CT = 2L;

        HBaseTransactionManager tm = (HBaseTransactionManager) newTransactionManager(context);

        // Pre-load the element to look for in the cache
        Table htable = hBaseUtils.getConnection().getTable(TableName.valueOf(TEST_TABLE));
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(new HTableAccessWrapper(htable, htable),
                tm.getCommitTableClient());
        TTable table = new TTable(htable, snapshotFilter, false);

        HBaseCellId hBaseCellId = HBaseCellId.valueOf(tm.getConflictDetectionLevel(), table, row1, family, qualifier, CELL_ST);
        Map<Long, Long> fakeCache = Maps.newHashMap();
        fakeCache.put(CELL_ST, CELL_CT);

        // Then test that locator finds it in the cache
        CommitTimestampLocator ctLocator = new CommitTimestampLocatorImpl(hBaseCellId, fakeCache);
        CommitTimestamp ct = snapshotFilter.locateCellCommitTimestamp(CELL_ST, tm.tsoClient.getEpoch(), ctLocator,
                false);
        assertTrue(ct.isValid());
        assertEquals(ct.getValue(), CELL_CT);
        assertTrue(ct.getLocation().compareTo(CACHE) == 0);

    }

    // Tests step 2 in AbstractTransactionManager.locateCellCommitTimestamp()
    // Note: This test is very similar to testCrashAfterCommit() above so
    // maybe we should merge them in this test, adding the missing assertions
    @Test(timeOut = 30_000)
    public void testCellCommitTimestampIsLocatedInCommitTable(ITestContext context) throws Exception {

        PostCommitActions syncPostCommitter =
                spy(new HBaseSyncPostCommitter(new NullMetricsProvider(), getCommitTable(context).getClient()));
        AbstractTransactionManager tm = (AbstractTransactionManager) newTransactionManager(context, syncPostCommitter);
        // The following line emulates a crash after commit that is observed in (*) below
        doThrow(new RuntimeException()).when(syncPostCommitter).updateShadowCells(any(HBaseTransaction.class));

        Table htable = connection.getTable(TableName.valueOf(TEST_TABLE));
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(new HTableAccessWrapper(htable, htable),
                tm.getCommitTableClient());

        try (TTable table = spy(new TTable(htable, snapshotFilter, false))) {
            // Commit a transaction that is broken on commit to avoid
            // write to the shadow cells and avoid cleaning the commit table
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            Put put = new Put(row1);
            put.addColumn(family, qualifier, data1);
            table.put(tx1, put);
            try {
                tm.commit(tx1);
            } catch (Exception e) { // (*) crash
                // Do nothing
            }

            // Test the locator finds the appropriate data in the commit table
            HBaseCellId hBaseCellId = HBaseCellId.valueOf(tx1, table, row1, family, qualifier,
                    tx1.getStartTimestamp());
            CommitTimestampLocator ctLocator = new CommitTimestampLocatorImpl(hBaseCellId,
                    Maps.<Long, Long>newHashMap());
            CommitTimestamp ct = snapshotFilter.locateCellCommitTimestamp(tx1.getStartTimestamp(), tm.tsoClient.getEpoch(),
                    ctLocator, false);
            assertTrue(ct.isValid());
            long expectedCommitTS = tx1.getStartTimestamp() + AbstractTransactionManager.MAX_CHECKPOINTS_PER_TXN;
            assertEquals(ct.getValue(), expectedCommitTS);
            assertTrue(ct.getLocation().compareTo(COMMIT_TABLE) == 0);
        }

    }

    // Tests step 3 in AbstractTransactionManager.locateCellCommitTimestamp()
    @Test(timeOut = 30_000)
    public void testCellCommitTimestampIsLocatedInShadowCells(ITestContext context) throws Exception {

        HBaseTransactionManager tm = (HBaseTransactionManager) newTransactionManager(context);

        Table htable = connection.getTable(TableName.valueOf(TEST_TABLE));
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(new HTableAccessWrapper(htable, htable),
                tm.getCommitTableClient());

        try (TTable table = spy(new TTable(htable, snapshotFilter, false))) {
            // Commit a transaction to addColumn ST/CT in commit table
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            Put put = new Put(row1);
            put.addColumn(family, qualifier, data1);
            table.put(tx1, put);
            tm.commit(tx1);
            // Upon commit, the commit data should be in the shadow cells

            // Test the locator finds the appropriate data in the shadow cells
            HBaseCellId hBaseCellId = HBaseCellId.valueOf(tx1, table, row1, family, qualifier,
                    tx1.getStartTimestamp());
            CommitTimestampLocator ctLocator = new CommitTimestampLocatorImpl(hBaseCellId,
                    Maps.<Long, Long>newHashMap());
            CommitTimestamp ct = snapshotFilter.locateCellCommitTimestamp(tx1.getStartTimestamp(), tm.tsoClient.getEpoch(),
                    ctLocator, false);
            assertTrue(ct.isValid());
            assertEquals(ct.getValue(), tx1.getCommitTimestamp());
            assertTrue(ct.getLocation().compareTo(SHADOW_CELL) == 0);
        }

    }

    // Tests step 4 in AbstractTransactionManager.locateCellCommitTimestamp()
    @Test(timeOut = 30_000)
    public void testCellFromTransactionInPreviousEpochGetsInvalidComitTimestamp(ITestContext context) throws Exception {

        final long CURRENT_EPOCH_FAKE = 1000L * AbstractTransactionManager.MAX_CHECKPOINTS_PER_TXN;

        CommitTable.Client commitTableClient = spy(getCommitTable(context).getClient());
        AbstractTransactionManager tm = spy((AbstractTransactionManager) newTransactionManager(context, commitTableClient));
        // The following lines allow to reach step 4)
        // in AbstractTransactionManager.locateCellCommitTimestamp()
        SettableFuture<Optional<CommitTimestamp>> f = SettableFuture.create();
        f.set(Optional.<CommitTimestamp>absent());
        doReturn(f).when(commitTableClient).getCommitTimestamp(any(Long.class));

        Table htable = connection.getTable(TableName.valueOf(TEST_TABLE));
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(new HTableAccessWrapper(htable, htable),
                tm.getCommitTableClient());

        try (TTable table = spy(new TTable(htable, snapshotFilter, false))) {

            // Commit a transaction to addColumn ST/CT in commit table
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            Put put = new Put(row1);
            put.addColumn(family, qualifier, data1);
            table.put(tx1, put);
            // Upon commit, the commit data should be in the shadow cells

            // Test a transaction in the previous epoch gets an InvalidCommitTimestamp class
            HBaseCellId hBaseCellId = HBaseCellId.valueOf(tx1, table, row1, family, qualifier,
                    tx1.getStartTimestamp());
            CommitTimestampLocator ctLocator = new CommitTimestampLocatorImpl(hBaseCellId,
                    Maps.<Long, Long>newHashMap());
            // Fake the current epoch to simulate a newer TSO
            CommitTimestamp ct = snapshotFilter.locateCellCommitTimestamp(tx1.getStartTimestamp(), CURRENT_EPOCH_FAKE,
                    ctLocator, false);
            assertFalse(ct.isValid());
            assertEquals(ct.getValue(), CommitTable.INVALID_TRANSACTION_MARKER);
            assertTrue(ct.getLocation().compareTo(COMMIT_TABLE) == 0);
        }
    }

    // Tests step 5 in AbstractTransactionManager.locateCellCommitTimestamp()
    @Test(timeOut = 30_000)
    public void testCellCommitTimestampIsLocatedInCommitTableAfterNotBeingInvalidated(ITestContext context) throws Exception {

        CommitTable.Client commitTableClient = spy(getCommitTable(context).getClient());
        PostCommitActions syncPostCommitter =
                spy(new HBaseSyncPostCommitter(new NullMetricsProvider(), commitTableClient));
        AbstractTransactionManager tm = spy((AbstractTransactionManager) newTransactionManager(context, syncPostCommitter));

        // The following line emulates a crash after commit that is observed in (*) below
        doThrow(new RuntimeException()).when(syncPostCommitter).updateShadowCells(any(HBaseTransaction.class));
        // The next two lines avoid steps 2) and 3) and go directly to step 5)
        // in AbstractTransactionManager.locateCellCommitTimestamp()
        SettableFuture<Optional<CommitTimestamp>> f = SettableFuture.create();
        f.set(Optional.<CommitTimestamp>absent());
        doReturn(f).doCallRealMethod().when(commitTableClient).getCommitTimestamp(any(Long.class));

        Table htable = connection.getTable(TableName.valueOf(TEST_TABLE));
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(new HTableAccessWrapper(htable, htable),
                tm.getCommitTableClient());

        try (TTable table = spy(new TTable(htable, snapshotFilter, false))) {

            // Commit a transaction that is broken on commit to avoid
            // write to the shadow cells and avoid cleaning the commit table
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            Put put = new Put(row1);
            put.addColumn(family, qualifier, data1);
            table.put(tx1, put);
            try {
                tm.commit(tx1);
            } catch (Exception e) { // (*) crash
                // Do nothing
            }

            // Test the locator finds the appropriate data in the commit table
            HBaseCellId hBaseCellId = HBaseCellId.valueOf(tx1, table, row1, family, qualifier,
                    tx1.getStartTimestamp());
            CommitTimestampLocator ctLocator = new CommitTimestampLocatorImpl(hBaseCellId,
                    Maps.<Long, Long>newHashMap());
            CommitTimestamp ct = snapshotFilter.locateCellCommitTimestamp(tx1.getStartTimestamp(), tm.tsoClient.getEpoch(),
                    ctLocator, false);
            assertTrue(ct.isValid());
            assertEquals(ct.getValue(), tx1.getCommitTimestamp());
            assertTrue(ct.getLocation().compareTo(COMMIT_TABLE) == 0);
        }

    }

    // Tests step 6 in AbstractTransactionManager.locateCellCommitTimestamp()
    @Test(timeOut = 30_000)
    public void testCellCommitTimestampIsLocatedInShadowCellsAfterNotBeingInvalidated(ITestContext context) throws Exception {

        CommitTable.Client commitTableClient = spy(getCommitTable(context).getClient());
        AbstractTransactionManager tm = spy((AbstractTransactionManager) newTransactionManager(context, commitTableClient));
        // The next two lines avoid steps 2), 3) and 5) and go directly to step 6)
        // in AbstractTransactionManager.locateCellCommitTimestamp()
        SettableFuture<Optional<CommitTimestamp>> f = SettableFuture.create();
        f.set(Optional.<CommitTimestamp>absent());
        doReturn(f).when(commitTableClient).getCommitTimestamp(any(Long.class));

        Table htable = connection.getTable(TableName.valueOf(TEST_TABLE));
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(new HTableAccessWrapper(htable, htable),
                tm.getCommitTableClient());

        try (TTable table = spy(new TTable(htable, snapshotFilter, false))) {

            // Commit a transaction to addColumn ST/CT in commit table
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            Put put = new Put(row1);
            put.addColumn(family, qualifier, data1);
            table.put(tx1, put);
            tm.commit(tx1);
            // Upon commit, the commit data should be in the shadow cells

            // Test the locator finds the appropriate data in the shadow cells
            HBaseCellId hBaseCellId = HBaseCellId.valueOf(tx1, table, row1, family, qualifier,
                    tx1.getStartTimestamp());
            CommitTimestampLocator ctLocator = new CommitTimestampLocatorImpl(hBaseCellId,
                    Maps.<Long, Long>newHashMap());
            CommitTimestamp ct = snapshotFilter.locateCellCommitTimestamp(tx1.getStartTimestamp(), tm.tsoClient.getEpoch(),
                    ctLocator,false);
            assertTrue(ct.isValid());
            assertEquals(ct.getValue(), tx1.getCommitTimestamp());
            assertTrue(ct.getLocation().compareTo(SHADOW_CELL) == 0);
        }

    }

    // Tests last step in AbstractTransactionManager.locateCellCommitTimestamp()
    @Test(timeOut = 30_000)
    public void testCTLocatorReturnsAValidCTWhenNotPresent(ITestContext context) throws Exception {

        final long CELL_TS = 1L;

        CommitTable.Client commitTableClient = spy(getCommitTable(context).getClient());
        AbstractTransactionManager tm = spy((AbstractTransactionManager) newTransactionManager(context, commitTableClient));
        // The following lines allow to reach the last return statement
        SettableFuture<Optional<CommitTimestamp>> f = SettableFuture.create();
        f.set(Optional.<CommitTimestamp>absent());
        doReturn(f).when(commitTableClient).getCommitTimestamp(any(Long.class));

        Table htable = connection.getTable(TableName.valueOf(TEST_TABLE));
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(new HTableAccessWrapper(htable, htable),
                tm.getCommitTableClient());

        try (TTable table = spy(new TTable(htable, snapshotFilter, false))) {
            HBaseCellId hBaseCellId = HBaseCellId.valueOf(((HBaseTransactionManager)tm).getConflictDetectionLevel(), table, row1, family, qualifier, CELL_TS);
            CommitTimestampLocator ctLocator = new CommitTimestampLocatorImpl(hBaseCellId,
                    Maps.<Long, Long>newHashMap());
            CommitTimestamp ct = snapshotFilter.locateCellCommitTimestamp(CELL_TS, tm.tsoClient.getEpoch(),
                    ctLocator, false);
            assertTrue(ct.isValid());
            assertEquals(ct.getValue(), -1L);
            assertTrue(ct.getLocation().compareTo(NOT_PRESENT) == 0);
        }
    }




}
