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

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.omid.transaction.AbstractTransaction.VisibilityLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HBaseTransaction extends AbstractTransaction<HBaseCellId> {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseTransaction.class);

    public HBaseTransaction(long transactionId, long epoch, Set<HBaseCellId> writeSet, Set<HBaseCellId> conflictFreeWriteSet, AbstractTransactionManager tm) {
        super(transactionId, epoch, writeSet, conflictFreeWriteSet, tm);
    }

    public HBaseTransaction(long transactionId, long readTimestamp, VisibilityLevel visibilityLevel, long epoch, Set<HBaseCellId> writeSet, Set<HBaseCellId> conflictFreeWriteSet, AbstractTransactionManager tm) {
        super(transactionId, readTimestamp, visibilityLevel, epoch, writeSet, conflictFreeWriteSet, tm);
    }

    private void deleteCell(HBaseCellId cell) {
        Delete delete = new Delete(cell.getRow());
        delete.deleteColumn(cell.getFamily(), cell.getQualifier(), cell.getTimestamp());
        try {
            cell.getTable().delete(delete);
        } catch (IOException e) {
            LOG.warn("Failed cleanup cell {} for Tx {}. This issue has been ignored", cell, getTransactionId(), e);
        }
    }
    @Override
    public void cleanup() {
        for (final HBaseCellId cell : getWriteSet()) {
            deleteCell(cell);
        }

        for (final HBaseCellId cell : getConflictFreeWriteSet()) {
            deleteCell(cell);
        }
        try {
            flushTables();
        } catch (IOException e) {
            LOG.warn("Failed flushing tables for Tx {}", getTransactionId(), e);
        }
    }

    /**
     * Flushes pending operations for tables touched by transaction
     * @throws IOException in case of any I/O related issues
     */
    public void flushTables() throws IOException {

        for (HTableInterface writtenTable : getWrittenTables()) {
            writtenTable.flushCommits();
        }

    }

    // ****************************************************************************************************************
    // Helper methods
    // ****************************************************************************************************************

    private Set<HTableInterface> getWrittenTables() {
        HashSet<HBaseCellId> writeSet = (HashSet<HBaseCellId>) getWriteSet();
        Set<HTableInterface> tables = new HashSet<HTableInterface>();
        for (HBaseCellId cell : writeSet) {
            tables.add(cell.getTable());
        }
        writeSet = (HashSet<HBaseCellId>) getConflictFreeWriteSet();
        for (HBaseCellId cell : writeSet) {
            tables.add(cell.getTable());
        }
        return tables;
    }

}
