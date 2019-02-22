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

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;


import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.hbase.HBaseCommitTable;
import org.apache.omid.committable.hbase.HBaseCommitTableConfig;
import org.apache.omid.proto.TSOProto;
import org.apache.omid.transaction.AbstractTransaction.VisibilityLevel;
import org.apache.omid.HBaseShims;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionAccessWrapper;
import org.apache.hadoop.hbase.regionserver.RegionConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.omid.committable.hbase.HBaseCommitTableConfig.COMMIT_TABLE_NAME_KEY;

/**
 * Server side filtering to identify the transaction snapshot.
 */
public class OmidSnapshotFilter extends BaseRegionObserver {

    private static final Logger LOG = LoggerFactory.getLogger(OmidSnapshotFilter.class);

    private HBaseCommitTableConfig commitTableConf = null;
    private RegionCoprocessorEnvironment env = null;
    private Queue<SnapshotFilterImpl> snapshotFilterQueue = new ConcurrentLinkedQueue<>();
    private Map<Object, SnapshotFilterImpl> snapshotFilterMap = new ConcurrentHashMap<>();
    private CommitTable.Client inMemoryCommitTable = null;
    private CommitTable commitTable;

    public OmidSnapshotFilter(CommitTable.Client commitTableClient) {
        LOG.info("Compactor coprocessor initialized");
        this.inMemoryCommitTable = commitTableClient;
    }

    public OmidSnapshotFilter() {
        LOG.info("Compactor coprocessor initialized via empty constructor");
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        LOG.info("Starting snapshot filter coprocessor");
        this.env = (RegionCoprocessorEnvironment)env;
        commitTableConf = new HBaseCommitTableConfig();
        String commitTableName = env.getConfiguration().get(COMMIT_TABLE_NAME_KEY);
        if (commitTableName != null) {
            commitTableConf.setTableName(commitTableName);
        }
        LOG.info("Snapshot filter started");
        commitTable = new HBaseCommitTable(RegionConnectionFactory
                .getConnection(RegionConnectionFactory.ConnectionType.READ_CONNECTION, (RegionCoprocessorEnvironment) env),
                commitTableConf);
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        LOG.info("Snapshot filter stopped");
    }


    // Don't add an @Override tag since this method doesn't exist in both hbase-1 and hbase-2
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) {
        SnapshotFilterImpl snapshotFilter = snapshotFilterMap.get(get);
        if (snapshotFilter != null) {
            snapshotFilterQueue.add(snapshotFilter);
        }
    }


    // Don't add an @Override tag since this method doesn't exist in both hbase-1 and hbase-2
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)
            throws IOException {

        if (get.getAttribute(CellUtils.CLIENT_GET_ATTRIBUTE) == null) return;
        boolean isLowLatency = Bytes.toBoolean(get.getAttribute(CellUtils.LL_ATTRIBUTE));
        boolean isRowCD = Bytes.toBoolean(get.getAttribute(CellUtils.ROW_LEVEL_CONFLICTS_ATTRIBUTE));
        HBaseTransactionManager.ConflictDetectionLevel cdLevel = isRowCD? HBaseTransactionManager.ConflictDetectionLevel.ROW:
                HBaseTransactionManager.ConflictDetectionLevel.CELL;
        HBaseTransaction hbaseTransaction = getHBaseTransaction(get.getAttribute(CellUtils.TRANSACTION_ATTRIBUTE),
                isLowLatency, cdLevel);
        SnapshotFilterImpl snapshotFilter = getSnapshotFilter(e);
        snapshotFilterMap.put(get, snapshotFilter);

        get.setMaxVersions();
        Filter newFilter = TransactionFilters.getVisibilityFilter(get.getFilter(),
                snapshotFilter, hbaseTransaction);
        get.setFilter(newFilter);
    }

    private SnapshotFilterImpl getSnapshotFilter(ObserverContext<RegionCoprocessorEnvironment> e)
            throws IOException {
        SnapshotFilterImpl snapshotFilter= snapshotFilterQueue.poll();
        if (snapshotFilter == null) {
            RegionAccessWrapper regionAccessWrapper =
                    new RegionAccessWrapper(HBaseShims.getRegionCoprocessorRegion(e.getEnvironment()));
            snapshotFilter = new SnapshotFilterImpl(regionAccessWrapper, initAndGetCommitTableClient());
        }
        return snapshotFilter;
    }


    // Don't add an @Override tag since this method doesn't exist in both hbase-1 and hbase-2
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e,
                                        Scan scan,
                                        RegionScanner s) throws IOException {
        preScannerOpen(e,scan);
        return s;
    }


    // Don't add an @Override tag since this method doesn't exist in both hbase-1 and hbase-2
    public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e,
                               Scan scan) throws IOException {
        byte[] byteTransaction = scan.getAttribute(CellUtils.TRANSACTION_ATTRIBUTE);

        if (byteTransaction == null) {
            return;
        }
        boolean isLowLatency = Bytes.toBoolean(scan.getAttribute(CellUtils.LL_ATTRIBUTE));
        boolean isRowCD = Bytes.toBoolean(scan.getAttribute(CellUtils.ROW_LEVEL_CONFLICTS_ATTRIBUTE));
        HBaseTransactionManager.ConflictDetectionLevel cdLevel = isRowCD? HBaseTransactionManager.ConflictDetectionLevel.ROW:
                HBaseTransactionManager.ConflictDetectionLevel.CELL;
        HBaseTransaction hbaseTransaction = getHBaseTransaction(byteTransaction, isLowLatency,cdLevel);
        SnapshotFilterImpl snapshotFilter = getSnapshotFilter(e);

        scan.setMaxVersions();
        Filter newFilter = TransactionFilters.getVisibilityFilter(scan.getFilter(),
                snapshotFilter, hbaseTransaction);
        scan.setFilter(newFilter);
        return;
    }

    private HBaseTransaction getHBaseTransaction(byte[] byteTransaction, boolean isLowLatency, HBaseTransactionManager.ConflictDetectionLevel cdLevel)
            throws InvalidProtocolBufferException {
        TSOProto.Transaction transaction = TSOProto.Transaction.parseFrom(byteTransaction);
        long id = transaction.getTimestamp();
        long readTs = transaction.getReadTimestamp();
        long epoch = transaction.getEpoch();
        VisibilityLevel visibilityLevel = VisibilityLevel.fromInteger(transaction.getVisibilityLevel());

        return new HBaseTransaction(id, readTs, visibilityLevel, epoch, new HashSet<HBaseCellId>(), new HashSet<HBaseCellId>(), cdLevel,
                isLowLatency);

    }

    private CommitTable.Client initAndGetCommitTableClient() throws IOException {
        if (inMemoryCommitTable != null) {
            return inMemoryCommitTable;
        }
        CommitTable.Client commitTableClient = commitTable.getClient();
        return commitTableClient;
    }

}
