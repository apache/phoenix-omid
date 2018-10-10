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


import static com.google.common.base.Charsets.UTF_8;
import static org.apache.omid.committable.hbase.HBaseCommitTableConfig.DEFAULT_COMMIT_TABLE_CF_NAME;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.mockito.Matchers.any;

import com.google.common.base.Optional;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.hbase.KeyGenerator;
import org.apache.omid.committable.hbase.KeyGeneratorImplementations;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.tso.client.OmidClientConfiguration;
import org.apache.omid.tso.client.TSOClient;
import org.apache.omid.tso.client.TSOFuture;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.ITestContext;
import org.testng.annotations.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;


@Test(groups = "sharedHBase")
public class TestOmidLLRaces extends OmidTestBase {

    private static final byte[] row1 = Bytes.toBytes("test-is-committed1");
    private static final byte[] row2 = Bytes.toBytes("test-is-committed2");
    private static final byte[] family = Bytes.toBytes(TEST_FAMILY);
    private static final byte[] qualifier = Bytes.toBytes("testdata");
    private static final byte[] data1 = Bytes.toBytes("testWrite-1");

    private static final Logger LOG = LoggerFactory.getLogger(TestOmidLLRaces.class);
    @Override
    protected boolean isLowLatency() {
        return true;
    }

    @Test(timeOut = 30_000)
    public void testIsCommitted(ITestContext context) throws Exception {
        AbstractTransactionManager tm = (AbstractTransactionManager)newTransactionManagerHBaseCommitTable(getClient(context));

        Table htable = connection.getTable(TableName.valueOf(TEST_TABLE));
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(new HTableAccessWrapper(htable, htable),
                tm.getCommitTableClient());
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

        HBaseCellId hBaseCellId1 = new HBaseCellId(table, row1, family, qualifier, t1.getStartTimestamp());
        HBaseCellId hBaseCellId2 = new HBaseCellId(table, row2, family, qualifier, t2.getStartTimestamp());
        HBaseCellId hBaseCellId3 = new HBaseCellId(table, row2, family, qualifier, t3.getStartTimestamp());

        assertTrue(snapshotFilter.isCommitted(hBaseCellId1, 0, false), "row1 should be committed");
        assertFalse(snapshotFilter.isCommitted(hBaseCellId2, 0, false), "row2 should not be committed for kv2");
        assertTrue(snapshotFilter.isCommitted(hBaseCellId3, 0, false), "row2 should be committed for kv3");
        assertTrue(tm.isLowLatency());
    }


    @Test(timeOut = 30_000)
    public void testInvalidation(ITestContext context) throws Exception {
        AbstractTransactionManager tm = (AbstractTransactionManager)newTransactionManagerHBaseCommitTable(getClient(context));

        Table htable = connection.getTable(TableName.valueOf(TEST_TABLE));
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(new HTableAccessWrapper(htable, htable),
                tm.getCommitTableClient());
        TTable table = spy(new TTable(htable, snapshotFilter, false));

        HBaseTransaction t1 = (HBaseTransaction) tm.begin();
        Put put = new Put(row1);
        put.addColumn(family, qualifier, data1);
        table.put(t1, put);

        HBaseTransaction t2 = (HBaseTransaction) tm.begin();
        Get get = new Get(row1);
        get.addColumn(family, qualifier);
        table.get(t2,get);

        //assert there is an invalidation marker:
        Table commitTable = connection.getTable(TableName.valueOf("OMID_COMMIT_TABLE"));
        KeyGenerator keygen = KeyGeneratorImplementations.defaultKeyGenerator();
        byte[] row = keygen.startTimestampToKey(t1.getStartTimestamp());
        Get getInvalidation = new Get(row);
        getInvalidation.addColumn(Bytes.toBytes(DEFAULT_COMMIT_TABLE_CF_NAME),"IT".getBytes(UTF_8));
        Result res = commitTable.get(getInvalidation);
        int val = Bytes.toInt(res.getValue(Bytes.toBytes(DEFAULT_COMMIT_TABLE_CF_NAME), "IT".getBytes(UTF_8)));
        assertTrue(val == 1);

        boolean gotInvalidated = false;
        try {
            tm.commit(t1);
        } catch (RollbackException e) {
            gotInvalidated = true;
        }
        assertTrue(gotInvalidated);
        tm.commit(t2);
        Thread.sleep(1000);
        res = commitTable.get(getInvalidation);
        assertTrue(res.isEmpty());
        assertTrue(tm.isLowLatency());
    }

//    @Test(timeOut = 30_000000)
//    public void testReadRace(ITestContext context) throws Exception {
//
//        final CountDownLatch waitForCommit = new CountDownLatch(1);
//        final CountDownLatch latch1 = new CountDownLatch(1);
//        final CountDownLatch latch2 = new CountDownLatch(1);
//        final Integer[] shadowCellReads = {0};
//
//        AbstractTransactionManager tm = (AbstractTransactionManager)newTransactionManager(context);
//        Table htable = connection.getTable(TableName.valueOf(TEST_TABLE));
//        SnapshotFilterImpl snapshotFilter = spy(new SnapshotFilterImpl(new HTableAccessWrapper(htable, htable),
//                tm.getCommitTableClient()));
//        TTable table = spy(new TTable(htable, snapshotFilter, false));
//
//        doAnswer(new Answer<Optional<CommitTable.CommitTimestamp>>() {
//            @Override
//            public Optional<CommitTable.CommitTimestamp> answer(InvocationOnMock invocation) throws Throwable {
//                shadowCellReads[0]++;
//                if (shadowCellReads[0] == 1) {
//                    Optional<CommitTable.CommitTimestamp> result = (Optional<CommitTable.CommitTimestamp>) invocation.callRealMethod();
//                    latch1.countDown();
//                    latch2.await();
//                    return result;
//                } else {
//                    Optional<CommitTable.CommitTimestamp> result = (Optional<CommitTable.CommitTimestamp>) invocation.callRealMethod();
//                    latch1.countDown();
//                    latch2.await();
//                    return result;
//                }
//            }
//        }).when(snapshotFilter).readCommitTimestampFromShadowCell(any(long.class), any(CommitTimestampLocator.class));
//
//        doAnswer(new Answer<TSOFuture<Long>>() {
//            @Override
//            public TSOFuture<Long> answer(InvocationOnMock invocationOnMock) throws Throwable {
//
//                TSOFuture<Long> res = (TSOFuture<Long>)invocationOnMock.callRealMethod();
//
//                LOG.info("writer thread commit ts {}",1);
//                waitForCommit.countDown();
//                return res;
//            }
//        }).when(client).commit(any(long.class), any(Set.class));
//
//        Thread readThread = new Thread("Read Thread") {
//            @Override
//            public void run() {
//                try {
//                    waitForCommit.await();
//                    HBaseTransaction t2 = (HBaseTransaction) tm.begin();
//                    LOG.info("reader thread ts {}", t2.getStartTimestamp());
//                    Get get = new Get(row1);
//                    get.addColumn(family, qualifier);
//                    table.get(t2,get);
//                    tm.commit(t2);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                } catch (RollbackException e) {
//                    e.printStackTrace();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        };
//
//        HBaseTransaction t1 = (HBaseTransaction) tm.begin();
//        Put put = new Put(row1);
//        put.addColumn(family, qualifier, data1);
//        table.put(t1, put);
//
//        readThread.start();
//
//        //latch1.await();
//
//
//
//        boolean gotInvalidated = false;
//        try {
//            tm.commit(t1);
//        } catch (RollbackException e) {
//            gotInvalidated = true;
//        }
//
//        latch2.countDown();
//        assertFalse(gotInvalidated);
//
//
//        assertTrue(tm.isLowLatency());
//    }

    //TODO testfence
}
