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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import java.io.IOException;

@Test(groups = "sharedHBase")
public class TestTransactionConflict extends OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestTransactionConflict.class);

    @Test(timeOut = 10_000)
    public void runTestWriteWriteConflict(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        Transaction t2 = tm.begin();
        LOG.info("Transaction created" + t2);

        byte[] row = Bytes.toBytes("test-simple");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        Put p = new Put(row);
        p.addColumn(fam, col, data1);
        tt.put(t1, p);

        Put p2 = new Put(row);
        p2.addColumn(fam, col, data2);
        tt.put(t2, p2);

        tm.commit(t2);

        try {
            tm.commit(t1);
            fail("Transaction should not commit successfully");
        } catch (RollbackException e) {
        }
    }

    @Test(timeOut = 10_000)
    public void runTestMultiTableConflict(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        TTable tt = new TTable(connection, TEST_TABLE);
        String table2 = TEST_TABLE + 2;
        TableName table2Name = TableName.valueOf(table2);

        try (Connection conn = ConnectionFactory.createConnection(hbaseConf);
             Admin admin = conn.getAdmin()) {
            TableName htable2 = TableName.valueOf(table2);

            if (!admin.tableExists(htable2)) {
                ColumnFamilyDescriptor datafam = ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes(TEST_FAMILY))
                        .setMaxVersions(Integer.MAX_VALUE)
                        .build();
                TableDescriptor desc = TableDescriptorBuilder
                        .newBuilder(table2Name)
                        .setColumnFamily(datafam)
                        .build();
                admin.createTable(desc);
            }
    
            if (admin.isTableDisabled(htable2)) {
                admin.enableTable(htable2);
            }
        }

        TTable tt2 = new TTable(connection, table2);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        Transaction t2 = tm.begin();
        LOG.info("Transaction created" + t2);

        byte[] row = Bytes.toBytes("test-simple");
        byte[] row2 = Bytes.toBytes("test-simple2");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        Put p = new Put(row);
        p.addColumn(fam, col, data1);
        tt.put(t1, p);
        tt2.put(t1, p);

        Put p2 = new Put(row);
        p2.addColumn(fam, col, data2);
        tt.put(t2, p2);
        p2 = new Put(row2);
        p2.addColumn(fam, col, data2);
        tt2.put(t2, p2);

        tm.commit(t2);

        boolean aborted = false;
        try {
            tm.commit(t1);
            fail("Transaction commited successfully");
        } catch (RollbackException e) {
            aborted = true;
        }
        assertTrue(aborted, "Transaction didn't raise exception");

        ResultScanner rs = tt2.getHTable().getScanner(fam, col);

        int count = 0;
        Result r;
        while ((r = rs.next()) != null) {
            count += r.size();
        }
        assertEquals(count, 1, "Should have cell");
    }

    @Test(timeOut = 10_000)
    public void runTestCleanupAfterConflict(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        Transaction t2 = tm.begin();
        LOG.info("Transaction created" + t2);

        byte[] row = Bytes.toBytes("test-simple");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        Put p = new Put(row);
        p.addColumn(fam, col, data1);
        tt.put(t1, p);

        Get g = new Get(row).readAllVersions();
        g.addColumn(fam, col);
        Result r = tt.getHTable().get(g);
        assertEquals(r.size(), 1, "Unexpected size for read.");
        assertTrue(Bytes.equals(data1, r.getValue(fam, col)),
                   "Unexpected value for read: " + Bytes.toString(r.getValue(fam, col)));

        Put p2 = new Put(row);
        p2.addColumn(fam, col, data2);
        tt.put(t2, p2);

        r = tt.getHTable().get(g);
        assertEquals(r.size(), 2, "Unexpected size for read.");
        r = tt.get(t2, g);
        assertEquals(r.size(),1, "Unexpected size for read.");
        assertTrue(Bytes.equals(data2, r.getValue(fam, col)),
                   "Unexpected value for read: " + Bytes.toString(r.getValue(fam, col)));

        tm.commit(t1);

        boolean aborted = false;
        try {
            tm.commit(t2);
            fail("Transaction commited successfully");
        } catch (RollbackException e) {
            aborted = true;
        }
        assertTrue(aborted, "Transaction didn't raise exception");

        r = tt.getHTable().get(g);
        assertEquals(r.size(), 1, "Unexpected size for read.");
        assertTrue(Bytes.equals(data1, r.getValue(fam, col)),
                   "Unexpected value for read: " + Bytes.toString(r.getValue(fam, col)));
    }

    @Test(timeOut = 10_000)
    public void testCleanupWithDeleteRow(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context);
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        int rowcount = 10;
        int count = 0;

        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        byte[] modrow = Bytes.toBytes("test-del" + 3);
        for (int i = 0; i < rowcount; i++) {
            byte[] row = Bytes.toBytes("test-del" + i);

            Put p = new Put(row);
            p.addColumn(fam, col, data1);
            tt.put(t1, p);
        }
        tm.commit(t1);

        Transaction t2 = tm.begin();
        LOG.info("Transaction created " + t2);
        Delete d = new Delete(modrow);
        tt.delete(t2, d);

        ResultScanner rs = tt.getScanner(t2, new Scan());
        Result r = rs.next();
        count = 0;
        while (r != null) {
            count++;
            LOG.trace("row: " + Bytes.toString(r.getRow()) + " count: " + count);
            r = rs.next();
        }
        assertEquals(count, rowcount - 1, "Wrong count");

        Transaction t3 = tm.begin();
        LOG.info("Transaction created " + t3);
        Put p = new Put(modrow);
        p.addColumn(fam, col, data2);
        tt.put(t3, p);

        tm.commit(t3);

        boolean aborted = false;
        try {
            tm.commit(t2);
            fail("Didn't abort");
        } catch (RollbackException e) {
            aborted = true;
        }
        assertTrue(aborted, "Didn't raise exception");

        Transaction tscan = tm.begin();
        rs = tt.getScanner(tscan, new Scan());
        r = rs.next();
        count = 0;
        while (r != null) {
            count++;
            r = rs.next();
        }
        assertEquals(count, rowcount, "Wrong count");

    }

    private int countRows(Table table) throws IOException {
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        Result r = scanner.next();
        int rowCount = 0;
        while (r != null) {
            r = scanner.next();
            rowCount++;
        }
        return rowCount;
    }

    @Test(timeOut = 60_000)
    public void testBatchedCleanup(ITestContext context) throws Exception {

        String table2 = "testBatchedCleanupTABLE2";
        TableName table2Name = TableName.valueOf(table2);

        try (Connection conn = ConnectionFactory.createConnection(hbaseConf);
            Admin admin = conn.getAdmin()) {
            TableName htable2 = TableName.valueOf(table2);

            if (!admin.tableExists(htable2)) {
                ColumnFamilyDescriptor datafam = ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes(TEST_FAMILY))
                        .setMaxVersions(Integer.MAX_VALUE)
                        .build();
                TableDescriptor desc = TableDescriptorBuilder
                        .newBuilder(table2Name)
                        .setColumnFamily(datafam)
                        .build();
                admin.createTable(desc);
            }

            if (admin.isTableDisabled(htable2)) {
                admin.enableTable(htable2);
            }
        }

        TransactionManager tm = newTransactionManager(context);
        TTable tt = new TTable(connection, TEST_TABLE);
        TTable tt2 = new TTable(connection, table2);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        Transaction t2 = tm.begin();
        LOG.info("Transaction created" + t2);

        byte[] row = Bytes.toBytes("test-simple");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        Put p = new Put(row);
        p.addColumn(fam, col, data1);
        tt.put(t1, p);

        Get g = new Get(row).readAllVersions();
        g.addColumn(fam, col);
        Result r = tt.getHTable().get(g);
        assertEquals(r.size(), 1, "Unexpected size for read.");
        assertTrue(Bytes.equals(data1, r.getValue(fam, col)),
                "Unexpected value for read: " + Bytes.toString(r.getValue(fam, col)));

        int rowcount = HBaseTransaction.MAX_DELETE_BATCH_SIZE*2 + 2;

        // Add this row to cause conflict
        Put p2 = new Put(row);
        p2.addColumn(fam, col, data2);
        tt.put(t2, p2);

        //Add more rows to hit batch
        for (int i = 0; i < rowcount; i++) {
            byte[] newRow = Bytes.toBytes("test-del" + i);
            Put put = new Put(newRow);
            put.addColumn(fam, col, data2);
            tt.put(t2, put);
            tt2.put(t2, put);
        }

        // validate rows are really written
        assertEquals(countRows(tt.getHTable()), rowcount + 1, "Unexpected size for read.");
        assertEquals(countRows(tt2.getHTable()), rowcount, "Unexpected size for read.");

        tm.commit(t1);

        boolean aborted = false;
        try {
            tm.commit(t2);
            fail("Transaction commited successfully");
        } catch (RollbackException e) {
            aborted = true;
        }
        assertTrue(aborted, "Transaction didn't raise exception");

        // validate rows are cleaned
        assertEquals(countRows(tt.getHTable()), 1, "Unexpected size for read.");
        assertEquals(countRows(tt2.getHTable()), 0, "Unexpected size for read.");
    }


    @Test(timeOut = 10_000)
    public void testMultipleCellChangesOnSameRow(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction t1 = tm.begin();
        Transaction t2 = tm.begin();
        LOG.info("Transactions created " + t1 + " " + t2);

        byte[] row = Bytes.toBytes("row");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col1 = Bytes.toBytes("testdata1");
        byte[] col2 = Bytes.toBytes("testdata2");
        byte[] data = Bytes.toBytes("testWrite-1");

        Put p2 = new Put(row);
        p2.addColumn(fam, col1, data);
        tt.put(t2, p2);
        tm.commit(t2);

        Put p1 = new Put(row);
        p1.addColumn(fam, col2, data);
        tt.put(t1, p1);
        tm.commit(t1);
    }

    @Test(timeOut = 10_000)
    public void runTestWriteWriteConflictWithAdditionalConflictFreeWrites(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        TTable tt1 = new TTable(connection, TEST_TABLE);
        TTable tt2 = new TTable(connection, TEST_TABLE, true);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        Transaction t2 = tm.begin();
        LOG.info("Transaction created" + t2);

        byte[] row = Bytes.toBytes("test-simple");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        Put p = new Put(row);
        p.addColumn(fam, col, data1);
        tt1.put(t1, p);

        Put p2 = new Put(row);
        p2.addColumn(fam, col, data2);
        tt1.put(t2, p2);

        row = Bytes.toBytes("test-simple-cf");
        p = new Put(row);
        p.addColumn(fam, col, data1);
        tt2.put(t1, p);

        p2 = new Put(row);
        p2.addColumn(fam, col, data2);
        tt2.put(t2, p2);

        tm.commit(t2);

        try {
            tm.commit(t1);
            fail("Transaction should not commit successfully");
        } catch (RollbackException e) {
        }
    }

    @Test(timeOut = 10_000)
    public void runTestWriteWriteConflictFreeWrites(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        TTable tt1 = new TTable(connection, TEST_TABLE);
        TTable tt2 = new TTable(connection, TEST_TABLE, true);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        Transaction t2 = tm.begin();
        LOG.info("Transaction created" + t2);

        byte[] row = Bytes.toBytes("test-simple");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        Put p = new Put(row);
        p.addColumn(fam, col, data1);
        tt1.put(t1, p);

        Put p2 = new Put(row);
        p2.addColumn(fam, col, data2);
        tt2.put(t2, p2);

        row = Bytes.toBytes("test-simple-cf");
        p = new Put(row);
        p.addColumn(fam, col, data1);
        tt1.put(t1, p);

        p2 = new Put(row);
        p2.addColumn(fam, col, data2);
        tt2.put(t2, p2);

        tm.commit(t2);

        try {
            tm.commit(t1);
        } catch (RollbackException e) {
            fail("Transaction should not commit successfully");
        }
    }

    @Test(timeOut = 10_000)
    public void runTestWriteWriteConflictFreeWritesWithOtherWrites(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        TTable tt1 = new TTable(connection, TEST_TABLE);
        TTable tt2 = new TTable(connection, TEST_TABLE, true);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        Transaction t2 = tm.begin();
        LOG.info("Transaction created" + t2);

        byte[] row = Bytes.toBytes("test-simple");
        byte[] row1 = Bytes.toBytes("test-simple-1");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        Put p = new Put(row);
        p.addColumn(fam, col, data1);
        tt1.put(t1, p);

        Put p2 = new Put(row1);
        p2.addColumn(fam, col, data2);
        tt1.put(t2, p2);

        row = Bytes.toBytes("test-simple-cf");
        p = new Put(row);
        p.addColumn(fam, col, data1);
        tt2.put(t1, p);

        p2 = new Put(row);
        p2.addColumn(fam, col, data2);
        tt2.put(t2, p2);

        tm.commit(t2);

        try {
            tm.commit(t1);
        } catch (RollbackException e) {
            fail("Transaction should not commit successfully");
        }
    }

    @Test(timeOut = 10_000)
    public void runTestCleanupConflictFreeWritesAfterConflict(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        TTable tt1 = new TTable(connection, TEST_TABLE);
        TTable tt2 = new TTable(connection, TEST_TABLE, true);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        Transaction t2 = tm.begin();
        LOG.info("Transaction created" + t2);

        byte[] row = Bytes.toBytes("test-simple");
        byte[] row1 = Bytes.toBytes("test-simple-1");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        Put p = new Put(row);
        p.addColumn(fam, col, data1);
        tt1.put(t1, p);

        Get g = new Get(row).readAllVersions();
        g.addColumn(fam, col);
        Result r = tt1.getHTable().get(g);
        assertEquals(r.size(), 1, "Unexpected size for read.");
        assertTrue(Bytes.equals(data1, r.getValue(fam, col)),
                   "Unexpected value for read: " + Bytes.toString(r.getValue(fam, col)));

        Put p2 = new Put(row);
        p2.addColumn(fam, col, data2);
        tt1.put(t2, p2);

        Put p3 = new Put(row1);
        p3.addColumn(fam, col, data2);
        tt2.put(t2, p3);

        r = tt1.getHTable().get(g);
        assertEquals(r.size(), 2, "Unexpected size for read.");
        r = tt2.get(t2, g);
        assertEquals(r.size(),1, "Unexpected size for read.");
        assertTrue(Bytes.equals(data2, r.getValue(fam, col)),
                   "Unexpected value for read: " + Bytes.toString(r.getValue(fam, col)));

        Get g1 = new Get(row1).readAllVersions();
        g1.addColumn(fam, col);
        r = tt1.getHTable().get(g1);
        assertEquals(r.size(), 1, "Unexpected size for read.");

        tm.commit(t1);

        boolean aborted = false;
        try {
            tm.commit(t2);
            fail("Transaction commited successfully");
        } catch (RollbackException e) {
            aborted = true;
        }
        assertTrue(aborted, "Transaction didn't raise exception");

        r = tt1.getHTable().get(g);
        assertEquals(r.size(), 1, "Unexpected size for read.");
        assertTrue(Bytes.equals(data1, r.getValue(fam, col)),
                   "Unexpected value for read: " + Bytes.toString(r.getValue(fam, col)));
        r = tt1.getHTable().get(g1);
        assertEquals(r.size(), 0, "Unexpected size for read.");
    }
}
