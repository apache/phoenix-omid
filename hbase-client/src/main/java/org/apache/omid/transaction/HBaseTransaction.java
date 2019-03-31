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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTransaction extends AbstractTransaction<HBaseCellId> {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseTransaction.class);
    static final int MAX_DELETE_BATCH_SIZE = 1000;
    
    public HBaseTransaction(long transactionId, long epoch, Set<HBaseCellId> writeSet,
                            Set<HBaseCellId> conflictFreeWriteSet, AbstractTransactionManager tm, boolean isLowLatency) {
        super(transactionId, epoch, writeSet, conflictFreeWriteSet, tm, isLowLatency);
    }

    public HBaseTransaction(long transactionId, long epoch, Set<HBaseCellId> writeSet,
                            Set<HBaseCellId> conflictFreeWriteSet, AbstractTransactionManager tm,
                            long readTimestamp, long writeTimestamp, boolean isLowLatency) {
        super(transactionId, epoch, writeSet, conflictFreeWriteSet, tm, readTimestamp, writeTimestamp, isLowLatency);
    }

    public HBaseTransaction(long transactionId, long readTimestamp, VisibilityLevel visibilityLevel, long epoch,
                            Set<HBaseCellId> writeSet, Set<HBaseCellId> conflictFreeWriteSet,
                            AbstractTransactionManager tm, boolean isLowLatency) {
        super(transactionId, readTimestamp, visibilityLevel, epoch, writeSet, conflictFreeWriteSet, tm, isLowLatency);
    }


    private void flushMutations(Table table, List<Mutation> mutations) throws IOException, InterruptedException {
        table.batch(mutations, new Object[mutations.size()]);
    }

    private void deleteCell(HBaseCellId cell, Map<Table,List<Mutation>> mutations) throws IOException, InterruptedException {

        Delete delete = new Delete(cell.getRow());
        delete.addColumn(cell.getFamily(), cell.getQualifier(), cell.getTimestamp());

        Table table = cell.getTable().getHTable();
        List<Mutation> tableMutations = mutations.get(table);
        if (tableMutations == null) {
            ArrayList<Mutation> newList = new ArrayList<>();
            newList.add(delete);
            mutations.put(table, newList);
        } else {
            tableMutations.add(delete);
            if (tableMutations.size() > MAX_DELETE_BATCH_SIZE) {
                flushMutations(table, tableMutations);
                mutations.remove(table);
            }
        }
    }

    @Override
    public void cleanup() {

        Map<Table,List<Mutation>> mutations = new HashMap<>();

        try {
            for (final HBaseCellId cell : getWriteSet()) {
                deleteCell(cell, mutations);
            }

            for (final HBaseCellId cell : getConflictFreeWriteSet()) {
                deleteCell(cell, mutations);
            }

            for (Map.Entry<Table,List<Mutation>> entry: mutations.entrySet()) {
                flushMutations(entry.getKey(), entry.getValue());
            }

        } catch (InterruptedException | IOException e) {
            LOG.warn("Failed cleanup for Tx {}. This issue has been ignored", getTransactionId(), e);
        }
    }

    /**
     * Flushes pending operations for tables touched by transaction
     * @throws IOException in case of any I/O related issues
     */
    public void flushTables() throws IOException {

        for (TTable writtenTable : getWrittenTables()) {
            writtenTable.flushCommits();
        }

    }

    // ****************************************************************************************************************
    // Helper methods
    // ****************************************************************************************************************

    private Set<TTable> getWrittenTables() {
        HashSet<HBaseCellId> writeSet = (HashSet<HBaseCellId>) getWriteSet();
        Set<TTable> tables = new HashSet<TTable>();
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
