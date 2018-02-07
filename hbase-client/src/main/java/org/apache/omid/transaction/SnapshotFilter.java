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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.omid.committable.CommitTable.CommitTimestamp;

import com.google.common.base.Optional;

public interface SnapshotFilter {
    
    public Result get(TTable ttable, Get get, HBaseTransaction transaction) throws IOException;

    public ResultScanner getScanner(TTable ttable, Scan scan, HBaseTransaction transaction) throws IOException;

    public List<Cell> filterCellsForSnapshot(List<Cell> rawCells, HBaseTransaction transaction,
            int versionsToRequest, Map<String, List<Cell>> familyDeletionCache, Map<String,byte[]> attributeMap) throws IOException;

    public boolean isCommitted(HBaseCellId hBaseCellId, long epoch) throws TransactionException;

    public CommitTimestamp locateCellCommitTimestamp(long cellStartTimestamp, long epoch,
            CommitTimestampLocator locator) throws IOException;

    public Optional<CommitTimestamp> readCommitTimestampFromShadowCell(long cellStartTimestamp, CommitTimestampLocator locator)
            throws IOException;

}
