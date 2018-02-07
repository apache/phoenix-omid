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

import com.google.common.annotations.VisibleForTesting;

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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.OmidRegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionAccessWrapper;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static org.apache.omid.committable.hbase.HBaseCommitTableConfig.COMMIT_TABLE_NAME_KEY;

/**
 * Server side filtering to identify the transaction snapshot.
 */
public class OmidSnapshotFilter extends BaseRegionObserver {

    private static final Logger LOG = LoggerFactory.getLogger(OmidSnapshotFilter.class);

    private HBaseCommitTableConfig commitTableConf = null;
    private Configuration conf = null;
    @VisibleForTesting
    private CommitTable.Client commitTableClient;

    private SnapshotFilterImpl snapshotFilter;

    final static String OMID_SNAPSHOT_FILTER_CF_FLAG = "OMID_SNAPSHOT_FILTER_ENABLED";

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
        commitTableClient = initAndGetCommitTableClient();
        
        snapshotFilter = new SnapshotFilterImpl(commitTableClient);
        
        LOG.info("Snapshot filter started");
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        LOG.info("Stopping snapshot filter coprocessor");
        commitTableClient.close();
        LOG.info("Snapshot filter stopped");
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get, List<Cell> result) throws IOException {

        if (get.getAttribute(CellUtils.CLIENT_GET_ATTRIBUTE) == null) return;

        get.setAttribute(CellUtils.CLIENT_GET_ATTRIBUTE, null);
        RegionAccessWrapper regionAccessWrapper = new RegionAccessWrapper(HBaseShims.getRegionCoprocessorRegion(c.getEnvironment()));
        Result res = regionAccessWrapper.get(get); // get parameters were set at the client side

        snapshotFilter.setTableAccessWrapper(regionAccessWrapper);

        List<Cell> filteredKeyValues = Collections.emptyList();
        if (!res.isEmpty()) {
            TSOProto.Transaction transaction = TSOProto.Transaction.parseFrom(get.getAttribute(CellUtils.TRANSACTION_ATTRIBUTE));

            long id = transaction.getTimestamp();
            long readTs = transaction.getReadTimestamp();
            long epoch = transaction.getEpoch();
            VisibilityLevel visibilityLevel = VisibilityLevel.fromInteger(transaction.getVisibilityLevel());


            HBaseTransaction hbaseTransaction = new HBaseTransaction(id, readTs, visibilityLevel, epoch, new HashSet<HBaseCellId>(), new HashSet<HBaseCellId>(), null);
            filteredKeyValues = snapshotFilter.filterCellsForSnapshot(res.listCells(), hbaseTransaction, get.getMaxVersions(), new HashMap<String, List<Cell>>(), get.getAttributesMap());
        }

        for (Cell cell : filteredKeyValues) {
            result.add(cell);
        }

        c.bypass();

    }

    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e,
            Scan scan,
            RegionScanner s) throws IOException {
        byte[] byteTransaction = scan.getAttribute(CellUtils.TRANSACTION_ATTRIBUTE);

        if (byteTransaction == null) {
            return s;
        }

        TSOProto.Transaction transaction = TSOProto.Transaction.parseFrom(byteTransaction);

        long id = transaction.getTimestamp();
        long readTs = transaction.getReadTimestamp();
        long epoch = transaction.getEpoch();
        VisibilityLevel visibilityLevel = VisibilityLevel.fromInteger(transaction.getVisibilityLevel());

        HBaseTransaction hbaseTransaction = new HBaseTransaction(id, readTs, visibilityLevel, epoch, new HashSet<HBaseCellId>(), new HashSet<HBaseCellId>(), null);

        RegionAccessWrapper regionAccessWrapper = new RegionAccessWrapper(HBaseShims.getRegionCoprocessorRegion(e.getEnvironment()));

        snapshotFilter.setTableAccessWrapper(regionAccessWrapper);

        return new OmidRegionScanner(snapshotFilter, s, hbaseTransaction, 1, scan.getAttributesMap());
    }

    private CommitTable.Client initAndGetCommitTableClient() throws IOException {
        LOG.info("Trying to get the commit table client");
        CommitTable commitTable = new HBaseCommitTable(conf, commitTableConf);
        CommitTable.Client commitTableClient = commitTable.getClient();
        LOG.info("Commit table client obtained {}", commitTableClient.getClass().getCanonicalName());
        return commitTableClient;
    }

}
