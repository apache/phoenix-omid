package org.apache.omid.transaction;


import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.fail;

@Test(groups = "sharedHBase")
public class TestRowLevelTSOConflict extends OmidTestBase {


    @Test
    public void testRowLevelConflict(ITestContext context) throws Exception {
        byte[] row1 = Bytes.toBytes("row123");
        byte[] qualifier = Bytes.toBytes("column123");
        byte[] qualifier2 = Bytes.toBytes("column123-2");
        byte[] data1 = Bytes.toBytes("data1");

        Table table1 = connection.getTable(TableName.valueOf(TEST_TABLE));
        TransactionManager tm = newTransactionManager(context, HBaseTransactionManager.ConflictDetectionLevel.ROW);
        TTable ttable1 = new TTable(table1);

        AbstractTransaction tx1 = (AbstractTransaction) tm.begin();
        Put put = new Put(row1);
        put.addColumn(Bytes.toBytes(TEST_FAMILY), qualifier, data1);
        ttable1.put(tx1, put);

        AbstractTransaction tx2 = (AbstractTransaction) tm.begin();
        Put put2 = new Put(row1);
        put2.addColumn(Bytes.toBytes(TEST_FAMILY), qualifier2, data1);
        ttable1.put(tx2, put2);

        tm.commit(tx1);
        try {
            tm.commit(tx2);
        } catch (RollbackException e) {
            return;
        }
        fail();
    }


    @Test
    public void testRowLevelNoConflict(ITestContext context) throws Exception {
        byte[] row1 = Bytes.toBytes("row123");
        byte[] row2 = Bytes.toBytes("row2-123");
        byte[] qualifier = Bytes.toBytes("column123");
        byte[] data1 = Bytes.toBytes("data1");

        Table table1 = connection.getTable(TableName.valueOf(TEST_TABLE));
        TransactionManager tm = newTransactionManager(context, HBaseTransactionManager.ConflictDetectionLevel.ROW);
        TTable ttable1 = new TTable(table1);

        AbstractTransaction tx1 = (AbstractTransaction) tm.begin();
        Put put = new Put(row1);
        put.addColumn(Bytes.toBytes(TEST_FAMILY), qualifier, data1);
        ttable1.put(tx1, put);

        AbstractTransaction tx2 = (AbstractTransaction) tm.begin();
        Put put2 = new Put(row2);
        put2.addColumn(Bytes.toBytes(TEST_FAMILY), qualifier, data1);
        ttable1.put(tx2, put2);

        tm.commit(tx1);
        try {
            tm.commit(tx2);
        } catch (RollbackException e) {
            fail();
        }

    }



    @Test
    public void testRowLevelConflict2Tables(ITestContext context) throws Exception {
        byte[] row1 = Bytes.toBytes("row123");
        byte[] qualifier = Bytes.toBytes("column123");
        byte[] data1 = Bytes.toBytes("data1");

        Table table1 = connection.getTable(TableName.valueOf(TEST_TABLE));
        TTable ttable1 = new TTable(table1);
        createTable("table2");
        Table table2 = connection.getTable(TableName.valueOf("table2"));
        TTable ttable2 = new TTable(table2);
        TransactionManager tm = newTransactionManager(context, HBaseTransactionManager.ConflictDetectionLevel.ROW);

        //Same put to be put in different tables
        Put put = new Put(row1);
        put.addColumn(Bytes.toBytes(TEST_FAMILY), qualifier, data1);
        Put put2 = new Put(row1);
        put2.addColumn(Bytes.toBytes(TEST_FAMILY), qualifier, data1);

        AbstractTransaction tx1 = (AbstractTransaction) tm.begin();
        ttable1.put(tx1, put);

        AbstractTransaction tx2 = (AbstractTransaction) tm.begin();
        ttable2.put(tx2, put2);

        tm.commit(tx1);
        try {
            tm.commit(tx2);
        } catch (RollbackException e) {
            fail();
        }
    }



}
