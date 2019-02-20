package org.apache.omid.transaction;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Set;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "sharedHBase")
public class TestHBaseCells extends OmidTestBase {






    @Test
    public void testCellModeFilter(ITestContext context) throws Exception {

        byte[] row1 = Bytes.toBytes("row123");
        byte[] qualifier = Bytes.toBytes("column123");
        byte[] qualifier2 = Bytes.toBytes("column123-2");
        byte[] data1 = Bytes.toBytes("data1");

        Table table1 = connection.getTable(TableName.valueOf(TEST_TABLE));
        TransactionManager tm = newTransactionManager(context);
        TTable ttable1 = new TTable(table1);

        AbstractTransaction tx1 = (AbstractTransaction) tm.begin();
        Put put = new Put(row1);
        put.addColumn(Bytes.toBytes(TEST_FAMILY), qualifier, data1);
        ttable1.put(tx1, put);
        ttable1.put(tx1, put);
        assertEquals(1, tx1.getWriteSet().size());

        //now add two from same row.
        AbstractTransaction tx2 = (AbstractTransaction) tm.begin();
        Put put2 = new Put(row1);
        put2.addColumn(Bytes.toBytes(TEST_FAMILY), qualifier, data1);
        put2.addColumn(Bytes.toBytes(TEST_FAMILY), qualifier2, data1);
        ttable1.put(tx2, put2);
        assertEquals(2,  tx2.getWriteSet().size());

        //add from different table
        deleteTable(hBaseUtils.getHBaseAdmin(), TableName.valueOf("table2"));
        createTable("table2");
        Table table2 = connection.getTable(TableName.valueOf("table2"));
        TTable ttable2 = new TTable(table2);

        AbstractTransaction tx3 = (AbstractTransaction) tm.begin();
        Put put3 = new Put(row1);
        put3.addColumn(Bytes.toBytes(TEST_FAMILY), qualifier, data1);

        ttable1.put(tx3, put3);
        ttable2.put(tx3, put3);
        assertEquals(2, tx3.getWriteSet().size());

    }


    @Test
    public void testRowModeFilter(ITestContext context) throws Exception {

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
        ttable1.put(tx1, put);
        assertEquals(1, tx1.getWriteSet().size());

        //now add two from same row.
        AbstractTransaction tx2 = (AbstractTransaction) tm.begin();
        Put put2 = new Put(row1);
        put2.addColumn(Bytes.toBytes(TEST_FAMILY), qualifier, data1);
        put2.addColumn(Bytes.toBytes(TEST_FAMILY), qualifier2, data1);
        ttable1.put(tx2, put2);
        assertEquals(1,  tx2.getWriteSet().size());

        //add from different table
        deleteTable(hBaseUtils.getHBaseAdmin(), TableName.valueOf("table2"));
        createTable("table2");
        Table table2 = connection.getTable(TableName.valueOf("table2"));
        TTable ttable2 = new TTable(table2);

        AbstractTransaction tx3 = (AbstractTransaction) tm.begin();
        Put put3 = new Put(row1);
        put3.addColumn(Bytes.toBytes(TEST_FAMILY), qualifier, data1);

        ttable1.put(tx3, put3);
        ttable2.put(tx3, put3);
        assertEquals(2, tx3.getWriteSet().size());

    }


    @Test
    public void testCellCompare() {

    }

}
