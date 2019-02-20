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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.Test;

@Test(groups = "sharedHBase")
public class TestDeletion extends OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestDeletion.class);

    private byte[] famA = Bytes.toBytes(TEST_FAMILY);
    private byte[] famB = Bytes.toBytes(TEST_FAMILY2);
    private byte[] colA = Bytes.toBytes("testdataA");
    private byte[] colB = Bytes.toBytes("testdataB");
    private byte[] data1 = Bytes.toBytes("testWrite-1");
    private byte[] modrow = Bytes.toBytes("test-del" + 0);

    private static class FamCol {

        final byte[] fam;
        final byte[] col;

        FamCol(byte[] fam, byte[] col) {
            this.fam = fam;
            this.col = col;
        }

    }

    private void printScanner(Table table, String s) throws IOException {
        System.out.println(s+"-----");
        ResultScanner realscanner = table.getScanner(new Scan());
        Result res = realscanner.next();
        while(res != null) {
            System.out.println("YONIGO");
            System.out.println(res);
            res = realscanner.next();
        }
        System.out.println(s+"------");
    }


    @Test(timeOut = 60_000)
    public void runTestDeleteFamilyRow(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context, HBaseTransactionManager.ConflictDetectionLevel.ROW);
        TTable tt = new TTable(connection, TEST_TABLE);


        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        int rowsWritten = 1;
        FamCol famColA = new FamCol(famA, colA);
        writeRows(tt, t1, rowsWritten, famColA);
        tm.commit(t1);

        printScanner(tt.getHTable(), "0");

        Transaction t2 = tm.begin();
        Delete d = new Delete(modrow);
        d.addFamily(famA);
        tt.delete(t2, d);

        printScanner(tt.getHTable(), "1");

        Transaction tscan = tm.begin();
        ResultScanner rs = tt.getScanner(tscan, new Scan());

        Map<FamCol, Integer> count = countColsInRows(rs, famColA);
        assertEquals((int) count.get(famColA), rowsWritten, "ColA count should be equal to rowsWritten");
        if (getClient(context).isLowLatency()) {
            return;
        }
        tm.commit(t2);




        tscan = tm.begin();
        rs = tt.getScanner(tscan, new Scan());

        count = countColsInRows(rs, famColA);
        Integer countFamColA = count.get(famColA);
        assertEquals(countFamColA, null);

        Transaction t3 = tm.begin();
        d.addFamily(famA);
        tt.delete(t3, d);

        tscan = tm.begin();
        rs = tt.getScanner(tscan, new Scan());

        count = countColsInRows(rs, famColA);
        countFamColA = count.get(famColA);
        assertEquals(countFamColA, null);
    }

    @Test(timeOut = 10_000)
    public void runTestDeleteFamilyCell(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context);
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        int rowsWritten = 1;
        FamCol famColA = new FamCol(famA, colA);
        writeRows(tt, t1, rowsWritten, famColA);
        tm.commit(t1);

        Transaction t2 = tm.begin();
        Delete d = new Delete(modrow);
        d.addFamily(famA);
        tt.delete(t2, d);

        Transaction tscan = tm.begin();
        ResultScanner rs = tt.getScanner(tscan, new Scan());

        Map<FamCol, Integer> count = countColsInRows(rs, famColA);
        assertEquals((int) count.get(famColA), rowsWritten, "ColA count should be equal to rowsWritten");
        if (getClient(context).isLowLatency()) {
            return;
        }
        tm.commit(t2);

        tscan = tm.begin();
        rs = tt.getScanner(tscan, new Scan());

        count = countColsInRows(rs, famColA);
        Integer countFamColA = count.get(famColA);
        assertEquals(countFamColA, null);

        Transaction t3 = tm.begin();
        d.addFamily(famA);
        tt.delete(t3, d);

        tscan = tm.begin();
        rs = tt.getScanner(tscan, new Scan());

        count = countColsInRows(rs, famColA);
        countFamColA = count.get(famColA);
        assertEquals(countFamColA, null);

    }

    @Test(timeOut = 10_000)
    public void runTestDeleteFamily(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context);
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        int rowsWritten = 10;
        FamCol famColA = new FamCol(famA, colA);
        FamCol famColB = new FamCol(famB, colB);
        writeRows(tt, t1, rowsWritten, famColA, famColB);
        tm.commit(t1);

        Transaction t2 = tm.begin();
        Delete d = new Delete(modrow);
        d.addFamily(famA);
        tt.delete(t2, d);

        Transaction tscan = tm.begin();
        ResultScanner rs = tt.getScanner(tscan, new Scan());

        Map<FamCol, Integer> count = countColsInRows(rs, famColA, famColB);
        assertEquals((int) count.get(famColA), rowsWritten, "ColA count should be equal to rowsWritten");
        assertEquals((int) count.get(famColB), rowsWritten, "ColB count should be equal to rowsWritten");
        if (getClient(context).isLowLatency()) {
            return;
        }
        tm.commit(t2);

        tscan = tm.begin();
        rs = tt.getScanner(tscan, new Scan());

        count = countColsInRows(rs, famColA, famColB);
        assertEquals((int) count.get(famColA), (rowsWritten - 1), "ColA count should be equal to rowsWritten - 1");
        assertEquals((int) count.get(famColB), rowsWritten, "ColB count should be equal to rowsWritten");
    }

    @Test(timeOut = 10_000)
    public void runTestDeleteFamilyRowLevelCA(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context, HBaseTransactionManager.ConflictDetectionLevel.ROW);
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        int rowsWritten = 10;
        FamCol famColA = new FamCol(famA, colA);
        FamCol famColB = new FamCol(famB, colB);
        writeRows(tt, t1, rowsWritten, famColA, famColB);
        tm.commit(t1);

        Transaction t2 = tm.begin();
        Delete d = new Delete(modrow);
        d.addFamily(famA);
        tt.delete(t2, d);

        Transaction tscan = tm.begin();
        ResultScanner rs = tt.getScanner(tscan, new Scan());

        Map<FamCol, Integer> count = countColsInRows(rs, famColA, famColB);
        assertEquals((int) count.get(famColA), rowsWritten, "ColA count should be equal to rowsWritten");
        assertEquals((int) count.get(famColB), rowsWritten, "ColB count should be equal to rowsWritten");
        if (getClient(context).isLowLatency()) {
            return;
        }
        tm.commit(t2);

        tscan = tm.begin();
        rs = tt.getScanner(tscan, new Scan());

        count = countColsInRows(rs, famColA, famColB);
        assertEquals((int) count.get(famColA), (rowsWritten - 1), "ColA count should be equal to rowsWritten - 1");
        assertEquals((int) count.get(famColB), rowsWritten, "ColB count should be equal to rowsWritten");

    }

    @Test(timeOut = 10_000)
    public void runTestDeleteFamilyAborts(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context, HBaseTransactionManager.ConflictDetectionLevel.ROW);
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        int rowsWritten = 10;
        FamCol famColA = new FamCol(famA, colA);
        FamCol famColB = new FamCol(famB, colB);
        writeRows(tt, t1, rowsWritten, famColA, famColB);

        Transaction t2 = tm.begin();

        tm.commit(t1);

        Delete d = new Delete(modrow);
        d.addFamily(famA);
        tt.delete(t2, d);

        try {
            tm.commit(t2);
        } catch(RollbackException e) {
            System.out.println("Rollback");
            System.out.flush();
        }

        Transaction tscan = tm.begin();
        ResultScanner rs = tt.getScanner(tscan, new Scan());

        Map<FamCol, Integer> count = countColsInRows(rs, famColA, famColB);
        assertEquals((int) count.get(famColA), rowsWritten, "ColA count should be equal to rowsWritten");
        assertEquals((int) count.get(famColB), rowsWritten, "ColB count should be equal to rowsWritten");

    }

    @Test(timeOut = 10_000)
    public void runTestDeleteColumn(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context);
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        int rowsWritten = 10;

        FamCol famColA = new FamCol(famA, colA);
        FamCol famColB = new FamCol(famA, colB);
        writeRows(tt, t1, rowsWritten, famColA, famColB);
        tm.commit(t1);

        Transaction t2 = tm.begin();
        Delete d = new Delete(modrow);
        d.addColumn(famA, colA);
        tt.delete(t2, d);

        Transaction tscan = tm.begin();
        ResultScanner rs = tt.getScanner(tscan, new Scan());

        Map<FamCol, Integer> count = countColsInRows(rs, famColA, famColB);
        assertEquals((int) count.get(famColA), rowsWritten, "ColA count should be equal to rowsWritten");
        assertEquals((int) count.get(famColB), rowsWritten, "ColB count should be equal to rowsWritten");

        if (getClient(context).isLowLatency()) {
            return;
        }

        tm.commit(t2);

        tscan = tm.begin();
        rs = tt.getScanner(tscan, new Scan());

        count = countColsInRows(rs, famColA, famColB);
        assertEquals((int) count.get(famColA), (rowsWritten - 1), "ColA count should be equal to rowsWritten - 1");
        assertEquals((int) count.get(famColB), rowsWritten, "ColB count should be equal to rowsWritten");

    }

    /**
     * This test is very similar to #runTestDeleteColumn() but exercises Delete#addColumns()
     */
    @Test(timeOut = 10_000)
    public void runTestDeleteColumns(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context);
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        int rowsWritten = 10;

        FamCol famColA = new FamCol(famA, colA);
        FamCol famColB = new FamCol(famA, colB);
        writeRows(tt, t1, rowsWritten, famColA, famColB);
        tm.commit(t1);

        Transaction t2 = tm.begin();
        Delete d = new Delete(modrow);
        d.addColumns(famA, colA);
        tt.delete(t2, d);

        Transaction tscan = tm.begin();
        ResultScanner rs = tt.getScanner(tscan, new Scan());

        Map<FamCol, Integer> count = countColsInRows(rs, famColA, famColB);
        assertEquals((int) count.get(famColA), rowsWritten, "ColA count should be equal to rowsWritten");
        assertEquals((int) count.get(famColB), rowsWritten, "ColB count should be equal to rowsWritten");
        if (getClient(context).isLowLatency()) {
            return;
        }
        tm.commit(t2);

        tscan = tm.begin();
        rs = tt.getScanner(tscan, new Scan());

        count = countColsInRows(rs, famColA, famColB);

        assertEquals((int) count.get(famColA), (rowsWritten - 1), "ColA count should be equal to rowsWritten - 1");
        assertEquals((int) count.get(famColB), rowsWritten, "ColB count should be equal to rowsWritten");

    }

    @Test(timeOut = 10_000)
    public void runTestDeleteRow(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        TTable tt = new TTable(connection, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        int rowsWritten = 10;

        FamCol famColA = new FamCol(famA, colA);
        writeRows(tt, t1, rowsWritten, famColA);

        tm.commit(t1);

        Transaction t2 = tm.begin();
        Delete d = new Delete(modrow);
        tt.delete(t2, d);

        Transaction tscan = tm.begin();
        ResultScanner rs = tt.getScanner(tscan, new Scan());

        int rowsRead = countRows(rs);
        assertTrue(rowsRead == rowsWritten, "Expected " + rowsWritten + " rows but " + rowsRead + " found");
        if (getClient(context).isLowLatency()) {
            return;
        }
        tm.commit(t2);

        tscan = tm.begin();
        rs = tt.getScanner(tscan, new Scan());

        rowsRead = countRows(rs);
        assertTrue(rowsRead == (rowsWritten - 1), "Expected " + (rowsWritten - 1) + " rows but " + rowsRead + " found");

    }

    @Test(timeOut = 10_000)
    public void testDeletionOfNonExistingColumnFamilyDoesNotWriteToHBase(ITestContext context) throws Exception {
        //TODO Debug why this test doesnt pass in low latency mode
        if (getClient(context).isLowLatency())
            return;
        // --------------------------------------------------------------------
        // Setup initial environment for the test
        // --------------------------------------------------------------------
        TransactionManager tm = newTransactionManager(context);
        TTable txTable = new TTable(connection, TEST_TABLE);

        Transaction tx1 = tm.begin();
        LOG.info("{} writing initial data created ", tx1);
        Put p = new Put(Bytes.toBytes("row1"));
        p.addColumn(famA, colA, data1);
        txTable.put(tx1, p);
        tm.commit(tx1);

        // --------------------------------------------------------------------
        // Try to delete a non existing CF
        // --------------------------------------------------------------------
        Transaction deleteTx = tm.begin();
        LOG.info("{} trying to delete a non-existing family created ", deleteTx);
        Delete del = new Delete(Bytes.toBytes("row1"));
        del.addFamily(famB);
        // This delete should not put data on HBase
        txTable.delete(deleteTx, del);

        // --------------------------------------------------------------------
        // Check data has not been written to HBase
        // --------------------------------------------------------------------
        Get get = new Get(Bytes.toBytes("row1"));
        get.setTimeStamp(deleteTx.getTransactionId());
        Result result = txTable.getHTable().get(get);
        assertTrue(result.isEmpty());

    }

    private int countRows(ResultScanner rs) throws IOException {
        int count;
        Result r = rs.next();
        count = 0;
        while (r != null) {
            count++;
            LOG.trace("row: " + Bytes.toString(r.getRow()) + " count: " + count);
            r = rs.next();
        }
        return count;
    }

    private void writeRows(TTable tt, Transaction t1, int rowcount, FamCol... famCols) throws IOException {
        for (int i = 0; i < rowcount; i++) {
            byte[] row = Bytes.toBytes("test-del" + i);

            Put p = new Put(row);
            for (FamCol col : famCols) {
                p.addColumn(col.fam, col.col, data1);
            }
            tt.put(t1, p);
        }
    }

    private Map<FamCol, Integer> countColsInRows(ResultScanner rs, FamCol... famCols) throws IOException {
        Map<FamCol, Integer> colCount = new HashMap<>();
        Result r = rs.next();
        while (r != null) {
            for (FamCol col : famCols) {
                if (r.containsColumn(col.fam, col.col)) {
                    Integer c = colCount.get(col);

                    if (c == null) {
                        colCount.put(col, 1);
                    } else {
                        colCount.put(col, c + 1);
                    }
                }
            }
            r = rs.next();
        }
        return colCount;
    }

}
