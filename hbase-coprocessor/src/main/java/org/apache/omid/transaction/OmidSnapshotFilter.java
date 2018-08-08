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
import org.apache.hadoop.hbase.filter.Filter;

import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.hbase.HBaseCommitTable;
import org.apache.omid.committable.hbase.HBaseCommitTableConfig;
import org.apache.omid.proto.TSOProto;
import org.apache.omid.transaction.AbstractTransaction.VisibilityLevel;
import org.apache.omid.HBaseShims;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionAccessWrapper;
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
    private Configuration conf = null;
    private Queue<SnapshotFilterImpl> snapshotFilterQueue = new ConcurrentLinkedQueue<>();
    private Map<Object, SnapshotFilterImpl> snapshotFilterMap = new ConcurrentHashMap();
    private CommitTable.Client inMemoryCommitTable = null;

    public OmidSnapshotFilter(CommitTable.Client commitTableClient) {
        LOG.info("Compactor coprocessor initialized with constructor for testing");
        this.inMemoryCommitTable = commitTableClient;
    }

    public OmidSnapshotFilter() {
        LOG.info("Compactor coprocessor initialized via empty constructor");
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        LOG.info("Starting snapshot filter coprocessor");
        conf = env.getConfiguration();
        commitTableConf = new HBaseCommitTableConfig();
        String commitTableName = conf.get(COMMIT_TABLE_NAME_KEY);
        if (commitTableName != null) {
            commitTableConf.setTableName(commitTableName);
        }
        LOG.info("Snapshot filter started");
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        LOG.info("Stopping snapshot filter coprocessor");
        if (snapshotFilterQueue != null) {
            for (SnapshotFilter snapshotFilter: snapshotFilterQueue) {
                ((SnapshotFilterImpl)snapshotFilter).getCommitTableClient().close();
            }
        }
        LOG.info("Snapshot filter stopped");
    }


    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)
            throws IOException {
        if (get.getFilter() != null) {
            //This get had a filter and used a commit table client that must put back
            assert (get.getFilter() instanceof TransactionVisibilityFilter);
            TransactionVisibilityFilter filter = (TransactionVisibilityFilter)get.getFilter();
            snapshotFilterQueue.add((SnapshotFilterImpl)filter.getSnapshotFilter());
        }
    }


    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)
            throws IOException {

        if (get.getAttribute(CellUtils.CLIENT_GET_ATTRIBUTE) == null) return;

        HBaseTransaction hbaseTransaction = getHBaseTransaction(get.getAttribute(CellUtils.TRANSACTION_ATTRIBUTE));
        SnapshotFilterImpl snapshotFilter = getSnapshotFilter(e);

        // In order to get hbase FilterBase framework to keep getting more versions
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


    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e,
                                         Scan scan,
                                         RegionScanner s) throws IOException {
        byte[] byteTransaction = scan.getAttribute(CellUtils.TRANSACTION_ATTRIBUTE);

        if (byteTransaction == null) {
            return s;
        }

        HBaseTransaction hbaseTransaction = getHBaseTransaction(byteTransaction);
        SnapshotFilterImpl snapshotFilter = getSnapshotFilter(e);

        scan.setMaxVersions();
        Filter newFilter = TransactionFilters.getVisibilityFilter(scan.getFilter(),
                snapshotFilter, hbaseTransaction);
        scan.setFilter(newFilter);
        snapshotFilterMap.put(scan, snapshotFilter);
        return s;
    }

    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e,
                                         Scan scan,
                                         RegionScanner s) throws IOException {
        byte[] byteTransaction = scan.getAttribute(CellUtils.TRANSACTION_ATTRIBUTE);

        if (byteTransaction == null) {
            return s;
        }

        SnapshotFilterImpl snapshotFilter = snapshotFilterMap.get(scan);
        assert(snapshotFilter != null);
        snapshotFilterMap.remove(scan);
        snapshotFilterMap.put(s, snapshotFilter);
        return s;
    }

    @Override
    public void preScannerClose(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s)
            throws IOException {
        SnapshotFilterImpl snapshotFilter = snapshotFilterMap.get(s);
        if (snapshotFilter != null) {
            snapshotFilterQueue.add(snapshotFilter);
        }
    }



    private HBaseTransaction getHBaseTransaction(byte[] byteTransaction)
            throws InvalidProtocolBufferException {
        TSOProto.Transaction transaction = TSOProto.Transaction.parseFrom(byteTransaction);
        long id = transaction.getTimestamp();
        long readTs = transaction.getReadTimestamp();
        long epoch = transaction.getEpoch();
        VisibilityLevel visibilityLevel = VisibilityLevel.fromInteger(transaction.getVisibilityLevel());

        return new HBaseTransaction(id, readTs, visibilityLevel, epoch, new HashSet<HBaseCellId>(), new HashSet<HBaseCellId>(), null);
    }

    private CommitTable.Client initAndGetCommitTableClient() throws IOException {
        LOG.info("Trying to get the commit table client");
        if (inMemoryCommitTable != null) {
            return inMemoryCommitTable;
        }
        CommitTable commitTable = new HBaseCommitTable(conf, commitTableConf);
        CommitTable.Client commitTableClient = commitTable.getClient();
        LOG.info("Commit table client obtained {}", commitTableClient.getClass().getCanonicalName());
        return commitTableClient;
    }

}
