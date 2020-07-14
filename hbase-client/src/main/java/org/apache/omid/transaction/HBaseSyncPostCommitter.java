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

import static org.apache.omid.metrics.MetricsUtils.name;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.Timer;
import org.apache.omid.tso.client.CellId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.SettableFuture;

public class HBaseSyncPostCommitter implements PostCommitActions {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseSyncPostCommitter.class);

    private final MetricsRegistry metrics;
    private final CommitTable.Client commitTableClient;

    private final Timer commitTableUpdateTimer;
    private final Timer shadowCellsUpdateTimer;
    static final int MAX_BATCH_SIZE=1000;
    private final Connection connection;

    public HBaseSyncPostCommitter(MetricsRegistry metrics, CommitTable.Client commitTableClient,
                                  Connection connection) {
        this.metrics = metrics;
        this.commitTableClient = commitTableClient;

        this.commitTableUpdateTimer = metrics.timer(name("omid", "tm", "hbase", "commitTableUpdate", "latency"));
        this.shadowCellsUpdateTimer = metrics.timer(name("omid", "tm", "hbase", "shadowCellsUpdate", "latency"));
        this.connection = connection;
    }

    private void flushMutations(TableName tableName, List<Mutation> mutations) throws IOException, InterruptedException {
        try (Table table = connection.getTable(tableName)){
            table.batch(mutations, new Object[mutations.size()]);
        }

    }

    private void addShadowCell(HBaseCellId cell, HBaseTransaction tx, SettableFuture<Void> updateSCFuture,
                               Map<TableName,List<Mutation>> mutations) throws IOException, InterruptedException {
        Put put = new Put(cell.getRow());
        put.addColumn(cell.getFamily(),
                CellUtils.addShadowCellSuffixPrefix(cell.getQualifier(), 0, cell.getQualifier().length),
                cell.getTimestamp(),
                Bytes.toBytes(tx.getCommitTimestamp()));

        TableName table = cell.getTable().getHTable().getName();
        List<Mutation> tableMutations = mutations.get(table);
        if (tableMutations == null) {
            ArrayList<Mutation> newList = new ArrayList<>();
            newList.add(put);
            mutations.put(table, newList);
        } else {
            tableMutations.add(put);
            if (tableMutations.size() > MAX_BATCH_SIZE) {
                flushMutations(table, tableMutations);
                mutations.remove(table);
            }
        }
    }

    @Override
    public ListenableFuture<Void> updateShadowCells(AbstractTransaction<? extends CellId> transaction) {

        SettableFuture<Void> updateSCFuture = SettableFuture.create();

        HBaseTransaction tx = HBaseTransactionManager.enforceHBaseTransactionAsParam(transaction);

        shadowCellsUpdateTimer.start();
        try {
            Map<TableName,List<Mutation>> mutations = new HashMap<>();
            // Add shadow cells
            for (HBaseCellId cell : tx.getWriteSet()) {
                addShadowCell(cell, tx, updateSCFuture, mutations);
            }

            for (HBaseCellId cell : tx.getConflictFreeWriteSet()) {
                addShadowCell(cell, tx, updateSCFuture, mutations);
            }

            for (Map.Entry<TableName,List<Mutation>> entry: mutations.entrySet()) {
                flushMutations(entry.getKey(), entry.getValue());
            }

            //Only if all is well we set to null and delete commit entry from commit table
            updateSCFuture.set(null);
        } catch (IOException | InterruptedException e) {
            LOG.warn("{}: Error inserting shadow cells", tx, e);
            updateSCFuture.setException(
                    new TransactionManagerException(tx + ": Error inserting shadow cells ", e));
        } finally {
            shadowCellsUpdateTimer.stop();
        }

        return updateSCFuture;

    }

    @Override
    public ListenableFuture<Void> removeCommitTableEntry(AbstractTransaction<? extends CellId> transaction) {

        SettableFuture<Void> updateSCFuture = SettableFuture.create();

        HBaseTransaction tx = HBaseTransactionManager.enforceHBaseTransactionAsParam(transaction);

        commitTableUpdateTimer.start();

        try {
            commitTableClient.deleteCommitEntry(tx.getStartTimestamp()).get();
            updateSCFuture.set(null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("{}: interrupted during commit table entry delete", tx, e);
            updateSCFuture.setException(
                    new TransactionManagerException(tx + ": interrupted during commit table entry delete"));
        } catch (ExecutionException e) {
            LOG.warn("{}: can't remove commit table entry", tx, e);
            updateSCFuture.setException(new TransactionManagerException(tx + ": can't remove commit table entry"));
        } finally {
            commitTableUpdateTimer.stop();
        }

        return updateSCFuture;

    }

}
