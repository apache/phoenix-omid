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

import static org.apache.omid.transaction.CellUtils.hasCell;
import static org.apache.omid.transaction.CellUtils.hasShadowCell;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.transaction.AbstractTransaction.VisibilityLevel;
import org.junit.Assert;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.Test;

@Test(groups = "sharedHBase")
public class TestCheckpoint extends OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestCheckpoint.class);

    private HBaseTransaction enforceHBaseTransactionAsParam(Transaction tx) {
        if (tx instanceof HBaseTransaction) {
            return (HBaseTransaction) tx;
        } else {
            throw new IllegalArgumentException(
                String.format("The transaction object passed %s is not an instance of HBaseTransaction",
                              tx.getClass().getName()));
        }
    }

    @Test(timeOut = 30_000)
    public void testFewCheckPoints(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context);
        TTable tt = new TTable(connection, TEST_TABLE);

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");
        byte[] dataValue2 = Bytes.toBytes("testWrite-2");
        byte[] dataValue3 = Bytes.toBytes("testWrite-3");

        Transaction tx1 = tm.begin();

        HBaseTransaction hbaseTx1 = enforceHBaseTransactionAsParam(tx1);

        Put row1 = new Put(rowName1);
        row1.addColumn(famName1, colName1, dataValue1);
        tt.put(tx1, row1);

        Get g = new Get(rowName1).setMaxVersions(1);

        Result r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue1, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        hbaseTx1.checkpoint();

        row1 = new Put(rowName1);
        row1.addColumn(famName1, colName1, dataValue2);
        tt.put(tx1, row1);

        r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue1, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        hbaseTx1.setVisibilityLevel(VisibilityLevel.SNAPSHOT);

        r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue2, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        hbaseTx1.checkpoint();

        row1 = new Put(rowName1);
        row1.addColumn(famName1, colName1, dataValue3);
        tt.put(tx1, row1);

        r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue2, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        hbaseTx1.checkpoint();

        r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue3, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        hbaseTx1.setVisibilityLevel(VisibilityLevel.SNAPSHOT_ALL);

        r = tt.get(tx1, g);
        
        assertTrue(r.size() == 3, "Expected 3 results and found " + r.size());

        List<Cell> cells = r.getColumnCells(famName1, colName1);
        assertTrue(Bytes.equals(dataValue3, CellUtil.cloneValue(cells.get(0))),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        assertTrue(Bytes.equals(dataValue2, CellUtil.cloneValue(cells.get(1))),
              "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        assertTrue(Bytes.equals(dataValue1, CellUtil.cloneValue(cells.get(2))),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        tt.close();
    }

    @Test(timeOut = 30_000)
    public void testSNAPSHOT(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        TTable tt = new TTable(connection, TEST_TABLE);

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue0 = Bytes.toBytes("testWrite-0");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");
        byte[] dataValue2 = Bytes.toBytes("testWrite-2");

        Transaction tx1 = tm.begin();

        Put row1 = new Put(rowName1);
        row1.addColumn(famName1, colName1, dataValue0);
        tt.put(tx1, row1);

        tm.commit(tx1);

        tx1 = tm.begin();

        HBaseTransaction hbaseTx1 = enforceHBaseTransactionAsParam(tx1);

        Get g = new Get(rowName1).setMaxVersions(1);

        Result r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue0, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        row1 = new Put(rowName1);
        row1.addColumn(famName1, colName1, dataValue1);
        tt.put(tx1, row1);


        r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue1, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        hbaseTx1.checkpoint();

        row1 = new Put(rowName1);
        row1.addColumn(famName1, colName1, dataValue2);
        tt.put(tx1, row1);

        r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue1, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        hbaseTx1.setVisibilityLevel(VisibilityLevel.SNAPSHOT);

        r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue2, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        tt.close();
    }
    
    @Test(timeOut = 30_000)
    public void testSNAPSHOT_ALL(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        TTable tt = new TTable(connection, TEST_TABLE);

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue0 = Bytes.toBytes("testWrite-0");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");
        byte[] dataValue2 = Bytes.toBytes("testWrite-2");

        Transaction tx1 = tm.begin();

        Put row1 = new Put(rowName1);
        row1.addColumn(famName1, colName1, dataValue0);
        tt.put(tx1, row1);

        tm.commit(tx1);

        tx1 = tm.begin();
        
        HBaseTransaction hbaseTx1 = enforceHBaseTransactionAsParam(tx1);

        Get g = new Get(rowName1).setMaxVersions(100);

        Result r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue0, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        row1 = new Put(rowName1);
        row1.addColumn(famName1, colName1, dataValue1);
        tt.put(tx1, row1);

        g = new Get(rowName1).setMaxVersions(100);

        r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue1, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        hbaseTx1.checkpoint();

        row1 = new Put(rowName1);
        row1.addColumn(famName1, colName1, dataValue2);
        tt.put(tx1, row1);

        r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue1, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        hbaseTx1.setVisibilityLevel(VisibilityLevel.SNAPSHOT_ALL);

        r = tt.get(tx1, g);
        
        assertTrue(r.size() == 3, "Expected 3 results and found " + r.size());

        List<Cell> cells = r.getColumnCells(famName1, colName1);
        assertTrue(Bytes.equals(dataValue2, CellUtil.cloneValue(cells.get(0))),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        assertTrue(Bytes.equals(dataValue1, CellUtil.cloneValue(cells.get(1))),
              "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        assertTrue(Bytes.equals(dataValue0, CellUtil.cloneValue(cells.get(2))),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        tt.close();
    }

    @Test(timeOut = 30_000)
    public void testSNAPSHOT_EXCLUDE_CURRENT(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        TTable tt = new TTable(connection, TEST_TABLE);

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");
        byte[] dataValue2 = Bytes.toBytes("testWrite-2");

        Transaction tx1 = tm.begin();

        HBaseTransaction hbaseTx1 = enforceHBaseTransactionAsParam(tx1);

        Put row1 = new Put(rowName1);
        row1.addColumn(famName1, colName1, dataValue1);
        tt.put(tx1, row1);

        Get g = new Get(rowName1).setMaxVersions(1);

        Result r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue1, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        hbaseTx1.checkpoint();

        row1 = new Put(rowName1);
        row1.addColumn(famName1, colName1, dataValue2);
        tt.put(tx1, row1);

        r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue1, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        hbaseTx1.setVisibilityLevel(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT);

        r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue1, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));
        
        tt.close();
    }

    @Test(timeOut = 30_000)
    public void testDeleteAfterCheckpoint(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        TTable tt = new TTable(connection, TEST_TABLE);

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");

        Transaction tx1 = tm.begin();

        Put row1 = new Put(rowName1);
        row1.addColumn(famName1, colName1, dataValue1);
        tt.put(tx1, row1);

        tm.commit(tx1);

        Transaction tx2 = tm.begin();

        HBaseTransaction hbaseTx2 = enforceHBaseTransactionAsParam(tx1);

        hbaseTx2.checkpoint();

        Delete d = new Delete(rowName1);
        tt.delete(tx2, d);

        try {
            tm.commit(tx2);
        } catch (TransactionException e) {
            Assert.fail();
        }

        tt.close();
    }

    @Test(timeOut = 30_000)
    public void testOutOfCheckpoints(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);

        Transaction tx1 = tm.begin();

        HBaseTransaction hbaseTx1 = enforceHBaseTransactionAsParam(tx1);

        for (int i = 0; i < CommitTable.MAX_CHECKPOINTS_PER_TXN - 1; ++i) {
            hbaseTx1.checkpoint();
        }

        try {
            hbaseTx1.checkpoint();
            Assert.fail();
        } catch (TransactionException e) {
            // expected
        }

    }


    @Test(timeOut = 60_000)
    public void testInMemoryCommitTableCheckpoints(ITestContext context) throws Exception {

        final byte[] row = Bytes.toBytes("test-sc");
        final byte[] family = Bytes.toBytes(TEST_FAMILY);
        final byte[] qualifier = Bytes.toBytes("testdata");
        final byte[] qualifier2 = Bytes.toBytes("testdata2");
        final byte[] data1 = Bytes.toBytes("testWrite-");

        final CountDownLatch beforeCTRemove = new CountDownLatch(1);
        final CountDownLatch afterCommit = new CountDownLatch(1);
        final CountDownLatch writerDone = new CountDownLatch(1);

        final AtomicLong startTimestamp = new AtomicLong(0);
        final AtomicLong commitTimestamp = new AtomicLong(0);
        PostCommitActions syncPostCommitter =
                spy(new HBaseSyncPostCommitter(new NullMetricsProvider(), getCommitTable(context).getClient(), connection));
        final AbstractTransactionManager tm = (AbstractTransactionManager) newTransactionManager(context, syncPostCommitter);

        Table htable = connection.getTable(TableName.valueOf(TEST_TABLE));
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(new HTableAccessWrapper(htable, htable),
                tm.getCommitTableClient());
        final TTable table = new TTable(htable,snapshotFilter);


        doAnswer(new Answer<ListenableFuture<Void>>() {
            @Override
            public ListenableFuture<Void> answer(InvocationOnMock invocation) throws Throwable {
                afterCommit.countDown();
                beforeCTRemove.await();
                ListenableFuture<Void> result = (ListenableFuture<Void>) invocation.callRealMethod();
                return result;
            }
        }).when(syncPostCommitter).removeCommitTableEntry(any(HBaseTransaction.class));


        Thread writeThread = new Thread("WriteThread"){
            @Override
            public void run() {
                try {

                    HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
                    Put put = new Put(row);
                    put.addColumn(family, qualifier, data1);

                    startTimestamp.set(tx1.getStartTimestamp());
                    table.put(tx1, put);
                    tx1.checkpoint();

                    Put put2 = new Put(row);
                    put2.addColumn(family, qualifier2, data1);
                    table.put(tx1, put2);

                    tm.commit(tx1);

                    commitTimestamp.set(tx1.getCommitTimestamp());
                    writerDone.countDown();
                } catch (IOException | RollbackException e) {
                    e.printStackTrace();
                }
            }
        };

        writeThread.start();

        afterCommit.await();

        Optional<CommitTable.CommitTimestamp> ct1 = tm.getCommitTableClient().getCommitTimestamp(startTimestamp.get()).get();
        Optional<CommitTable.CommitTimestamp> ct2 = tm.getCommitTableClient().getCommitTimestamp(startTimestamp.get() + 1).get();

        beforeCTRemove.countDown();

        writerDone.await();

        assertEquals(commitTimestamp.get(), ct1.get().getValue());
        assertEquals(commitTimestamp.get(), ct2.get().getValue());


        assertTrue(hasCell(row, family, qualifier, startTimestamp.get(), new TTableCellGetterAdapter(table)),
                "Cell should be there");
        assertTrue(hasCell(row, family, qualifier2, startTimestamp.get()+1, new TTableCellGetterAdapter(table)),
                "Cell should be there");
        assertTrue(hasShadowCell(row, family, qualifier, startTimestamp.get(), new TTableCellGetterAdapter(table)),
                "Cell should be there");
        assertTrue(hasShadowCell(row, family, qualifier2, startTimestamp.get()+1, new TTableCellGetterAdapter(table)),
                "Cell should be there");
    }
}
