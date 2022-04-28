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

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.hbase.HBaseCommitTable;
import org.apache.omid.committable.hbase.HBaseCommitTableConfig;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.CompactorScanner;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionConnectionFactory;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.omid.committable.hbase.HBaseCommitTableConfig.COMMIT_TABLE_NAME_KEY;

/**
 * Garbage collector for stale data: triggered upon HBase
 * compactions, it removes data from uncommitted transactions
 * older than the low watermark using a special scanner
 */
public class OmidCompactor extends BaseRegionObserver {

    private static final Logger LOG = LoggerFactory.getLogger(OmidCompactor.class);

    private static final String HBASE_RETAIN_NON_TRANSACTIONALLY_DELETED_CELLS_KEY
            = "omid.hbase.compactor.retain.tombstones";
    private static final boolean HBASE_RETAIN_NON_TRANSACTIONALLY_DELETED_CELLS_DEFAULT = true;

    final static String OMID_COMPACTABLE_CF_FLAG = "OMID_ENABLED";

    private boolean enableCompactorForAllFamilies = false;

    private HBaseCommitTableConfig commitTableConf = null;
    private RegionCoprocessorEnvironment env = null;

    @VisibleForTesting
    CommitTable.Client commitTableClient;

    // When compacting, if a cell which has been marked by HBase as Delete or
    // Delete Family (that is, non-transactionally deleted), we allow the user
    // to decide what the compactor scanner should do with it: retain it or not
    // If retained, the deleted cell will appear after a minor compaction, but
    // will be deleted anyways after a major one
    private boolean retainNonTransactionallyDeletedCells;

    private Connection connection;

    public OmidCompactor() {
        this(false);
    }

    public OmidCompactor(boolean enableCompactorForAllFamilies) {
        LOG.info("Compactor coprocessor initialized");
        this.enableCompactorForAllFamilies = enableCompactorForAllFamilies;
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        LOG.info("Starting compactor coprocessor");
        commitTableConf = new HBaseCommitTableConfig();
        String commitTableName = env.getConfiguration().get(COMMIT_TABLE_NAME_KEY);
        if (commitTableName != null) {
            commitTableConf.setTableName(commitTableName);
        }

        connection = RegionConnectionFactory
                .getConnection(RegionConnectionFactory.ConnectionType.COMPACTION_CONNECTION, (RegionCoprocessorEnvironment) env);
        commitTableClient = new HBaseCommitTable(connection, commitTableConf).getClient();
        retainNonTransactionallyDeletedCells =
                env.getConfiguration().getBoolean(HBASE_RETAIN_NON_TRANSACTIONALLY_DELETED_CELLS_KEY,
                        HBASE_RETAIN_NON_TRANSACTIONALLY_DELETED_CELLS_DEFAULT);
        LOG.info("Compactor coprocessor started");
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        LOG.info("Stopping compactor coprocessor");
        LOG.info("Compactor coprocessor stopped");
    }



    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> env,
                                      Store store,
                                      InternalScanner scanner,
                                      ScanType scanType,
                                      CompactionRequest request) throws IOException {
        boolean omidCompactable;
        try {
            if (enableCompactorForAllFamilies) {
                omidCompactable = true;
            } else {
                TableDescriptor desc = env.getEnvironment().getRegion().getTableDescriptor();
                ColumnFamilyDescriptor famDesc = desc.getColumnFamily(Bytes.toBytes(store.getColumnFamilyName()));
                omidCompactable =Boolean.valueOf(Bytes.toString(famDesc.getValue(Bytes.toBytes(OMID_COMPACTABLE_CF_FLAG))));
            }

            // only column families tagged as compactable are compacted
            // with omid compactor
            if (!omidCompactable) {
                return scanner;
            } else {
                boolean isMajorCompaction = request.isMajor();
                return new CompactorScanner(env,
                        scanner,
                        commitTableClient,
                        isMajorCompaction,
                        retainNonTransactionallyDeletedCells);
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new DoNotRetryIOException(e);
        }
    }
}
