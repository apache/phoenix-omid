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

import org.apache.phoenix.thirdparty.com.google.common.base.Optional;


import org.apache.commons.collections4.map.LRUMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.HashMap;

import java.util.List;
import java.util.Map;


public class TransactionVisibilityFilterBase extends FilterBase {

    // optional sub-filter to apply to visible cells
    private final Filter userFilter;
    private final SnapshotFilterImpl snapshotFilter;
    private final LRUMap<Long ,Long> commitCache;
    private final HBaseTransaction hbaseTransaction;

    // This cache is cleared when moving to the next row
    // So no need to keep row name
    private final Map<ImmutableBytesWritable, Long> familyDeletionCache;

    public TransactionVisibilityFilterBase(Filter cellFilter,
                                           SnapshotFilterImpl snapshotFilter,
                                           HBaseTransaction hbaseTransaction) {
        this.userFilter = cellFilter;
        this.snapshotFilter = snapshotFilter;
        commitCache = new LRUMap<>(1000);
        this.hbaseTransaction = hbaseTransaction;
        familyDeletionCache = new HashMap<>();

    }

    @Override
    public ReturnCode filterCell(Cell v) throws IOException {
        if (CellUtils.isShadowCell(v)) {
            Long commitTs =  Bytes.toLong(CellUtil.cloneValue(v));
            commitCache.put(v.getTimestamp(), commitTs);
            // Continue getting shadow cells until one of them fits this transaction
            if (hbaseTransaction.getStartTimestamp() >= commitTs) {
                return ReturnCode.NEXT_COL;
            } else {
                return ReturnCode.SKIP;
            }
        }

        Optional<Long> commitTS = getCommitIfInSnapshot(v, CellUtils.isFamilyDeleteCell(v));
        if (commitTS.isPresent()) {
            if (hbaseTransaction.getVisibilityLevel() == AbstractTransaction.VisibilityLevel.SNAPSHOT_ALL &&
                    snapshotFilter.getTSIfInTransaction(v, hbaseTransaction).isPresent()) {
                return runUserFilter(v, ReturnCode.INCLUDE);
            }
            if (CellUtils.isFamilyDeleteCell(v)) {
                familyDeletionCache.put(createImmutableBytesWritable(v), commitTS.get());
                if (hbaseTransaction.getVisibilityLevel() == AbstractTransaction.VisibilityLevel.SNAPSHOT_ALL) {
                    return runUserFilter(v, ReturnCode.INCLUDE_AND_NEXT_COL);
                } else {
                    return ReturnCode.NEXT_COL;
                }
            }
            Long deleteCommit = familyDeletionCache.get(createImmutableBytesWritable(v));
            if (deleteCommit != null && deleteCommit >= v.getTimestamp()) {
                if (hbaseTransaction.getVisibilityLevel() == AbstractTransaction.VisibilityLevel.SNAPSHOT_ALL) {
                    return runUserFilter(v, ReturnCode.INCLUDE_AND_NEXT_COL);
                } else {
                    return ReturnCode.NEXT_COL;
                }
            }
            if (CellUtils.isTombstone(v)) {
                if (hbaseTransaction.getVisibilityLevel() == AbstractTransaction.VisibilityLevel.SNAPSHOT_ALL) {
                    return runUserFilter(v, ReturnCode.INCLUDE_AND_NEXT_COL);
                } else {
                    return ReturnCode.NEXT_COL;
                }
            }

            return runUserFilter(v, ReturnCode.INCLUDE_AND_NEXT_COL);
        }

        return ReturnCode.SKIP;
    }


    private ImmutableBytesWritable createImmutableBytesWritable(Cell v) {
        return new ImmutableBytesWritable(v.getFamilyArray(),
                v.getFamilyOffset(),v.getFamilyLength());
    }

    private ReturnCode runUserFilter(Cell v, ReturnCode snapshotReturn)
            throws IOException {
        assert(snapshotReturn == ReturnCode.INCLUDE_AND_NEXT_COL || snapshotReturn == ReturnCode.INCLUDE);
        if (userFilter == null) {
            return snapshotReturn;
        }

        ReturnCode userRes = userFilter.filterCell(v);
        switch (userRes) {
            case INCLUDE:
                return snapshotReturn;
            case SKIP:
                return (snapshotReturn == ReturnCode.INCLUDE) ? ReturnCode.SKIP: ReturnCode.NEXT_COL;
            default:
                return userRes;
        }

    }

    // For family delete cells, the sc hasn't arrived yet so get sc from region before going to ct
    private Optional<Long> getCommitIfInSnapshot(Cell v, boolean getShadowCellBeforeCT) throws IOException {
        Long cachedCommitTS = commitCache.get(v.getTimestamp());
        if (cachedCommitTS != null) {
            if (hbaseTransaction.getStartTimestamp() >= cachedCommitTS) {
                return Optional.of(cachedCommitTS);
            } else {
                return Optional.absent();
            }
        }
        if (snapshotFilter.getTSIfInTransaction(v, hbaseTransaction).isPresent()) {
            return Optional.of(v.getTimestamp());
        }

        if (getShadowCellBeforeCT) {

            // Try to get shadow cell from region
            final Get get = new Get(CellUtil.cloneRow(v));
            get.setTimestamp(v.getTimestamp()).readVersions(1);
            get.addColumn(CellUtil.cloneFamily(v), CellUtils.addShadowCellSuffixPrefix(CellUtils.FAMILY_DELETE_QUALIFIER));
            Result shadowCell = snapshotFilter.getTableAccessWrapper().get(get);

            if (!shadowCell.isEmpty()) {
                long commitTS = Bytes.toLong(CellUtil.cloneValue(shadowCell.rawCells()[0]));
                commitCache.put(v.getTimestamp(), commitTS);
                if (commitTS <= hbaseTransaction.getStartTimestamp()) {
                    return Optional.of(commitTS);
                }
            }
        }

        Optional<Long> commitTS = snapshotFilter.getTSIfInSnapshot(v, hbaseTransaction, commitCache);
        if (commitTS.isPresent()) {
            commitCache.put(v.getTimestamp(), commitTS.get());
        } else {
            commitCache.put(v.getTimestamp(), Long.MAX_VALUE);
        }
        return commitTS;
    }


    @Override
    public void reset() throws IOException {
        familyDeletionCache.clear();
        if (userFilter != null) {
            userFilter.reset();
        }
    }

    @Override
    public boolean filterRow() throws IOException {
        if (userFilter != null) {
            return userFilter.filterRow();
        }
        return super.filterRow();
    }


    @Override
    public boolean filterRowKey(Cell cell) throws IOException {
        if (userFilter != null) {
            return userFilter.filterRowKey(cell);
        }
        return super.filterRowKey(cell);
    }

    @Override
    public boolean filterAllRemaining() throws IOException {
        if (userFilter != null) {
            return userFilter.filterAllRemaining();
        }
        return super.filterAllRemaining();
    }

    @Override
    public void filterRowCells(List<Cell> kvs) throws IOException {
        if (userFilter != null) {
            userFilter.filterRowCells(kvs);
        } else {
            super.filterRowCells(kvs);
        }
    }

    @Override
    public boolean hasFilterRow() {
        if (userFilter != null) {
            return userFilter.hasFilterRow();
        }
        return super.hasFilterRow();
    }

    @Override
    public Cell getNextCellHint(Cell currentKV) throws IOException {
        if (userFilter != null) {
            return userFilter.getNextCellHint(currentKV);
        }
        return super.getNextCellHint(currentKV);
    }

    @Override
    public boolean isFamilyEssential(byte[] name) throws IOException {
        if (userFilter != null) {
            return userFilter.isFamilyEssential(name);
        }
        return super.isFamilyEssential(name);
    }

    @Override
    public byte[] toByteArray() throws IOException {
        return super.toByteArray();
    }

    public Filter getInnerFilter() {
        return userFilter;
    }
}
