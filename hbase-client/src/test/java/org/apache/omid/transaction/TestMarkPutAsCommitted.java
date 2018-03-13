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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.omid.transaction.CellUtils.hasCell;
import static org.apache.omid.transaction.CellUtils.hasShadowCell;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(groups = "sharedHBase")
public class TestMarkPutAsCommitted extends OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestMarkPutAsCommitted.class);

    private static final String TEST_FAMILY = "data";

    static final byte[] row = Bytes.toBytes("test-sc");
    static final byte[] family = Bytes.toBytes(TEST_FAMILY);
    private static final byte[] qualifier = Bytes.toBytes("testdata-1");
    private static final byte[] data1 = Bytes.toBytes("testWrite-1");
    private static final byte[] data2 = Bytes.toBytes("testWrite-2");

    @Test(timeOut = 60_000)
    public void testShadowCellsExistanceInAutocommit(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context);

        TTable table = new TTable(hbaseConf, TEST_TABLE);

        HBaseTransaction t1 = (HBaseTransaction) tm.begin();

        // Test shadow cells are created properly
        Put put = new Put(row);
        put.add(family, qualifier, data1);
        
        put = TTable.markPutAsCommitted(put, t1.getWriteTimestamp(), t1.getWriteTimestamp());
      
        table.getHTable().put(put);

        // After markPutAsCommitted test that both cell and shadow cell are there
        assertTrue(hasCell(row, family, qualifier, t1.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                "Cell should be there");
        assertTrue(hasShadowCell(row, family, qualifier, t1.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                "Shadow cell should be there");
    }

    @Test(timeOut = 60_000)
    public void testReadAfterAutocommit(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context);

        TTable table = new TTable(hbaseConf, TEST_TABLE);

        HBaseTransaction t1 = (HBaseTransaction) tm.begin();

        Put put = new Put(row);
        put.add(family, qualifier, data1);

        table.put(t1, put);

        tm.commit(t1);

        // After commit test that both cell and shadow cell are there
        assertTrue(hasCell(row, family, qualifier, t1.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                "Cell should be there");
        assertTrue(hasShadowCell(row, family, qualifier, t1.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                "Shadow cell should be there");

        Transaction t2 = tm.begin();
        Get get = new Get(row);
        get.addColumn(family, qualifier);

        Result getResult = table.get(t2, get);
        assertTrue(Arrays.equals(data1, getResult.getValue(family, qualifier)), "Values should be the same");
        
        
        HBaseTransaction t3 = (HBaseTransaction) tm.begin();

        Put put1 = new Put(row);
        put1.add(family, qualifier, data2);

        put1 = TTable.markPutAsCommitted(put1, t3.getWriteTimestamp(), t3.getWriteTimestamp());

       table.getHTable().put(put1);

        // After markPutAsCommitted test that both cell and shadow cell are there
        assertTrue(hasCell(row, family, qualifier, t1.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                "Cell should be there");
        assertTrue(hasShadowCell(row, family, qualifier, t1.getStartTimestamp(), new TTableCellGetterAdapter(table)),
                "Shadow cell should be there");

        Transaction t4 = tm.begin();

        getResult = table.get(t4, get);
        //Test that t4 reads t3's write even though t3 was not committed 
        assertTrue(Arrays.equals(data2, getResult.getValue(family, qualifier)), "Values should be the same");
    }
}
