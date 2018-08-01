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

import com.google.common.base.Optional;
import com.sun.istack.Nullable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class TransactionVisibilityFilter extends FilterBase {

    // optional sub-filter to apply to visible cells
    private final Filter userFilter;
    private final SnapshotFilterImpl snapshotFilter;
    private final Map<Long ,Long> shadowCellCache;
    private final HBaseTransaction hbaseTransaction;
    private final Map<String, Long> familyDeletionCache;

    public SnapshotFilter getSnapshotFilter() {
        return snapshotFilter;
    }

    public TransactionVisibilityFilter(@Nullable Filter cellFilter,
                                       SnapshotFilterImpl snapshotFilter,
                                       HBaseTransaction hbaseTransaction) {
        this.userFilter = cellFilter;
        this.snapshotFilter = snapshotFilter;
        shadowCellCache = new HashMap<>();
        this.hbaseTransaction = hbaseTransaction;
        familyDeletionCache = new HashMap<String, Long>();
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        if (CellUtils.isShadowCell(v)) {
            Long commitTs =  Bytes.toLong(CellUtil.cloneValue(v));
            shadowCellCache.put(v.getTimestamp(), commitTs);
            // Continue getting shadow cells until one of them fits this transaction
            if (hbaseTransaction.getStartTimestamp() >= commitTs) {
                return ReturnCode.NEXT_COL;
            } else {
                return ReturnCode.SKIP;
            }
        } else if (CellUtils.isFamilyDeleteCell(v)) {
            //Delete is part of this transaction
            if (snapshotFilter.getTSIfInTransaction(v, hbaseTransaction).isPresent()) {
                familyDeletionCache.put(Bytes.toString(CellUtil.cloneFamily(v)), v.getTimestamp());
                return ReturnCode.NEXT_COL;
            }

            if (shadowCellCache.containsKey(v.getTimestamp()) &&
                    hbaseTransaction.getStartTimestamp() >= shadowCellCache.get(v.getTimestamp())) {
                familyDeletionCache.put(Bytes.toString(CellUtil.cloneFamily(v)), shadowCellCache.get(v.getTimestamp()));
                return ReturnCode.NEXT_COL;
            }

            // Try to get shadow cell from region
            final Get get = new Get(CellUtil.cloneRow(v));
            get.setTimeStamp(v.getTimestamp()).setMaxVersions(1);
            get.addColumn(CellUtil.cloneFamily(v), CellUtils.addShadowCellSuffix(CellUtils.FAMILY_DELETE_QUALIFIER));
            Result deleteFamilySC = snapshotFilter.getTableAccessWrapper().get(get);

            if (!deleteFamilySC.isEmpty() &&
                    Bytes.toLong(CellUtil.cloneValue(deleteFamilySC.rawCells()[0] )) < hbaseTransaction.getStartTimestamp()){
                familyDeletionCache.put(Bytes.toString(CellUtil.cloneFamily(v)), Bytes.toLong(CellUtil.cloneValue(deleteFamilySC.rawCells()[0])));
                return ReturnCode.NEXT_COL;
            }

            //At last go to commit table
            Optional<Long> commitTimestamp = snapshotFilter.tryToLocateCellCommitTimestamp(hbaseTransaction.getEpoch(),
                    v, shadowCellCache);
            if (commitTimestamp.isPresent() && hbaseTransaction.getStartTimestamp() >= commitTimestamp.get()) {
                familyDeletionCache.put(Bytes.toString(CellUtil.cloneFamily(v)), commitTimestamp.get());
                return ReturnCode.NEXT_COL;
            }

            // Continue getting the next version of the delete family,
            // until we get one in the snapshot or move to next cell
            return ReturnCode.SKIP;
        }

        if (familyDeletionCache.containsKey(Bytes.toString(CellUtil.cloneFamily(v)))
                && familyDeletionCache.get(Bytes.toString(CellUtil.cloneFamily(v))) >= v.getTimestamp()) {
            return ReturnCode.NEXT_COL;
        }

        if (isCellInSnapshot(v)) {

            if (CellUtils.isTombstone(v)) {
                return ReturnCode.NEXT_COL;
            }

            if (hbaseTransaction.getVisibilityLevel() == AbstractTransaction.VisibilityLevel.SNAPSHOT) {
                return runUserFilter(v, ReturnCode.INCLUDE_AND_NEXT_COL);
            } else if (hbaseTransaction.getVisibilityLevel() == AbstractTransaction.VisibilityLevel.SNAPSHOT_ALL) {
                if (snapshotFilter.getTSIfInTransaction(v, hbaseTransaction).isPresent()) {
                    return runUserFilter(v, ReturnCode.INCLUDE);
                } else {
                    return runUserFilter(v, ReturnCode.INCLUDE_AND_NEXT_COL);
                }
            }
        }
        return ReturnCode.SKIP;
    }


    private ReturnCode runUserFilter(Cell v, ReturnCode snapshotReturn)
            throws IOException {

        if (userFilter == null) {
            return snapshotReturn;
        }

        ReturnCode userRes = userFilter.filterKeyValue(v);
        switch (userRes) {
            case INCLUDE:
                return snapshotReturn;
            case SKIP:
                return (snapshotReturn == ReturnCode.INCLUDE) ? ReturnCode.SKIP: ReturnCode.NEXT_COL;
            default:
                return userRes;
        }

    }


    private boolean isCellInSnapshot(Cell v) throws IOException {
        if (shadowCellCache.containsKey(v.getTimestamp()) &&
                hbaseTransaction.getStartTimestamp() >= shadowCellCache.get(v.getTimestamp())) {
            return true;
        }
        if (snapshotFilter.getTSIfInTransaction(v, hbaseTransaction).isPresent()) {
            return true;
        }
        Optional<Long> commitTS = snapshotFilter.getTSIfInSnapshot(v, hbaseTransaction, shadowCellCache);
        if (commitTS.isPresent()) {
            shadowCellCache.put(v.getTimestamp(), commitTS.get());
            return true;
        }
        return false;
    }


    @Override
    public void reset() throws IOException {
        shadowCellCache.clear();
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
    public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
        if (userFilter != null) {
            return userFilter.filterRowKey(buffer, offset, length);
        }
        return super.filterRowKey(buffer, offset, length);
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
    public KeyValue getNextKeyHint(KeyValue currentKV) throws IOException {
        if (userFilter != null) {
            return userFilter.getNextKeyHint(currentKV);
        }
        return super.getNextKeyHint(currentKV);
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
}
