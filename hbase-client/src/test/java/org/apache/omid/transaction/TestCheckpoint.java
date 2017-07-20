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

import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.transaction.AbstractTransaction.VisibilityLevel;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

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
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");
        byte[] dataValue2 = Bytes.toBytes("testWrite-2");
        byte[] dataValue3 = Bytes.toBytes("testWrite-3");

        Transaction tx1 = tm.begin();

        HBaseTransaction hbaseTx1 = enforceHBaseTransactionAsParam(tx1);

        Put row1 = new Put(rowName1);
        row1.add(famName1, colName1, dataValue1);
        tt.put(tx1, row1);

        Get g = new Get(rowName1).setMaxVersions(1);

        Result r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue1, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        hbaseTx1.checkpoint();

        row1 = new Put(rowName1);
        row1.add(famName1, colName1, dataValue2);
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
        row1.add(famName1, colName1, dataValue3);
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
        assertTrue(Bytes.equals(dataValue3, cells.get(0).getValue()),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        assertTrue(Bytes.equals(dataValue2, cells.get(1).getValue()),
              "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        assertTrue(Bytes.equals(dataValue1, cells.get(2).getValue()),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        tt.close();
    }

    @Test(timeOut = 30_000)
    public void testSNAPSHOT(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue0 = Bytes.toBytes("testWrite-0");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");
        byte[] dataValue2 = Bytes.toBytes("testWrite-2");

        Transaction tx1 = tm.begin();

        Put row1 = new Put(rowName1);
        row1.add(famName1, colName1, dataValue0);
        tt.put(tx1, row1);

        tm.commit(tx1);

        tx1 = tm.begin();

        HBaseTransaction hbaseTx1 = enforceHBaseTransactionAsParam(tx1);

        Get g = new Get(rowName1).setMaxVersions(1);

        Result r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue0, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        row1 = new Put(rowName1);
        row1.add(famName1, colName1, dataValue1);
        tt.put(tx1, row1);


        r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue1, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        hbaseTx1.checkpoint();

        row1 = new Put(rowName1);
        row1.add(famName1, colName1, dataValue2);
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
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue0 = Bytes.toBytes("testWrite-0");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");
        byte[] dataValue2 = Bytes.toBytes("testWrite-2");

        Transaction tx1 = tm.begin();

        Put row1 = new Put(rowName1);
        row1.add(famName1, colName1, dataValue0);
        tt.put(tx1, row1);

        tm.commit(tx1);

        tx1 = tm.begin();
        
        HBaseTransaction hbaseTx1 = enforceHBaseTransactionAsParam(tx1);

        Get g = new Get(rowName1).setMaxVersions(100);

        Result r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue0, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        row1 = new Put(rowName1);
        row1.add(famName1, colName1, dataValue1);
        tt.put(tx1, row1);

        g = new Get(rowName1).setMaxVersions(100);

        r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue1, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        hbaseTx1.checkpoint();

        row1 = new Put(rowName1);
        row1.add(famName1, colName1, dataValue2);
        tt.put(tx1, row1);

        r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue1, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        hbaseTx1.setVisibilityLevel(VisibilityLevel.SNAPSHOT_ALL);

        r = tt.get(tx1, g);
        
        assertTrue(r.size() == 3, "Expected 3 results and found " + r.size());

        List<Cell> cells = r.getColumnCells(famName1, colName1);
        assertTrue(Bytes.equals(dataValue2, cells.get(0).getValue()),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        assertTrue(Bytes.equals(dataValue1, cells.get(1).getValue()),
              "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        assertTrue(Bytes.equals(dataValue0, cells.get(2).getValue()),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        tt.close();
    }

    @Test(timeOut = 30_000)
    public void testSNAPSHOT_EXCLUDE_CURRENT(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");
        byte[] dataValue2 = Bytes.toBytes("testWrite-2");

        Transaction tx1 = tm.begin();

        HBaseTransaction hbaseTx1 = enforceHBaseTransactionAsParam(tx1);

        Put row1 = new Put(rowName1);
        row1.add(famName1, colName1, dataValue1);
        tt.put(tx1, row1);

        Get g = new Get(rowName1).setMaxVersions(1);

        Result r = tt.get(tx1, g);
        assertTrue(Bytes.equals(dataValue1, r.getValue(famName1, colName1)),
                "Unexpected value for SI read " + tx1 + ": " + Bytes.toString(r.getValue(famName1, colName1)));

        hbaseTx1.checkpoint();

        row1 = new Put(rowName1);
        row1.add(famName1, colName1, dataValue2);
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
    public void testOutOfCheckpoints(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);

        Transaction tx1 = tm.begin();

        HBaseTransaction hbaseTx1 = enforceHBaseTransactionAsParam(tx1);

        for (int i=0; i < AbstractTransactionManager.NUM_OF_CHECKPOINTS ; ++i) {
            hbaseTx1.checkpoint();
        }

        try {
            hbaseTx1.checkpoint();
            Assert.fail();
        } catch (TransactionException e) {
            // expected
        }

    }
}
