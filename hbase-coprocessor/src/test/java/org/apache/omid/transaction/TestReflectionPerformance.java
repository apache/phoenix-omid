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

/*
 * This a quick benchmark to test the impact of using reflection to Handle the methods
 * missing from HBase 3.
 * 
 * The test is commented out, because it won't compile with HBase 3 (where the methods implemeneted
 * via reflection are never called anyway)
 * 
 * The results show that using reflection does not cause a performance hit
 * getNextCellHint() doesn't use reflection, and it is comparable in performance to the reflected
 * methods.
 * 
Testing with null filter (always call super)
10^8 filterRowKey(byte[]...) (super) invocations took: 1507 ms
10^8 filterRowKey (cell) invocations took: 1273 ms
10^8 getNextCellHint (cell) invocations took: 672 ms
Testing with dummy filter (Filter implements called method)
10^8 filterRowKey(byte[]...) invocations took: 959 ms
10^8 filterRowKey (cell) invocations took: 665 ms
10^8 getNextCellHint (cell) invocations took: 549 ms
 */
/*
package org.apache.omid.transaction;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

public class TestReflectionPerformance {

    public static void main(String[] args) throws IOException {
        compareReflectionPerformance();
    }

    static volatile int x;
    static volatile boolean r;
    static volatile Cell c;

    public static void compareReflectionPerformance() throws IOException {
        Cell cell =
                new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("q"),
                        Bytes.toBytes("v"));
        Filter nullFilter = new TransactionVisibilityFilter(null, null, null);
        long ts = new java.util.Date().getTime();
        byte[] buffer = cell.getRowArray();
        int offset = cell.getRowOffset();
        short length = cell.getRowLength();

        System.out.println("Testing with null filter (always call super)");

        for (int count = 100 * 1000 * 1000; count > 0; count--) {
            r = nullFilter.filterRowKey(buffer, offset, length);
        }
        long ts1 = new java.util.Date().getTime();
        System.out.println(
            "10^8 filterRowKey(byte[]...) (super) invocations took: " + (ts1 - ts) + " ms");

        for (int count = 100 * 1000 * 1000; count > 0; count--) {
            r = nullFilter.filterRowKey(cell);
        }
        long ts2 = new java.util.Date().getTime();
        System.out.println("10^8 filterRowKey (cell) invocations took: " + (ts2 - ts1) + " ms");

        for (int count = 100 * 1000 * 1000; count > 0; count--) {
            c = nullFilter.getNextCellHint(cell);
        }
        long ts3 = new java.util.Date().getTime();
        System.out.println("10^8 getNextCellHint (cell) invocations took: " + (ts3 - ts2) + " ms");

        System.out.println("Testing with dummy filter (Filter implements called method)");

        Filter dummyFilter = new TransactionVisibilityFilter(new TestFilter(), null, null);

        for (int count = 100 * 1000 * 1000; count > 0; count--) {
            r = dummyFilter.filterRowKey(buffer, offset, length);
        }
        long ts4 = new java.util.Date().getTime();
        System.out.println("10^8 filterRowKey(byte[]...) invocations took: " + (ts4 - ts3) + " ms");

        for (int count = 100 * 1000 * 1000; count > 0; count--) {
            r = dummyFilter.filterRowKey(cell);
        }
        long ts5 = new java.util.Date().getTime();
        System.out.println("10^8 filterRowKey (cell) invocations took: " + (ts5 - ts4) + " ms");

        for (int count = 100 * 1000 * 1000; count > 0; count--) {
            c = dummyFilter.getNextCellHint(cell);
        }
        long ts6 = new java.util.Date().getTime();
        System.out.println("10^8 getNextCellHint (cell) invocations took: " + (ts6 - ts5) + " ms");
    }

    public static class TestFilter extends FilterBase {

        @Override
        public boolean filterRowKey(Cell firstRowCell) {
            return false;
        }

        @Override
        public boolean filterRowKey(byte[] b, int o, int l) {
            return false;
        }

        @Override
        public Cell getNextCellHint(Cell cell) {
            return cell;
        }
    }

}
*/
