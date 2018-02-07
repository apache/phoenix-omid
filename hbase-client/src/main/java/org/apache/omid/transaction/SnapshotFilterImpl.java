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

import static org.apache.omid.committable.CommitTable.CommitTimestamp.Location.CACHE;
import static org.apache.omid.committable.CommitTable.CommitTimestamp.Location.COMMIT_TABLE;
import static org.apache.omid.committable.CommitTable.CommitTimestamp.Location.NOT_PRESENT;
import static org.apache.omid.committable.CommitTable.CommitTimestamp.Location.SHADOW_CELL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.CommitTable.CommitTimestamp;
import org.apache.omid.proto.TSOProto;
import org.apache.omid.transaction.AbstractTransaction.VisibilityLevel;
import org.apache.omid.transaction.HBaseTransactionManager.CommitTimestampLocatorImpl;
import org.apache.omid.transaction.TTable.TransactionalClientScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;

public class SnapshotFilterImpl implements SnapshotFilter {

    private static Logger LOG = LoggerFactory.getLogger(SnapshotFilterImpl.class);

    private TableAccessWrapper tableAccessWrapper;

    private CommitTable.Client commitTableClient;
    
    public SnapshotFilterImpl(TableAccessWrapper tableAccessWrapper, CommitTable.Client commitTableClient) throws IOException {
        this.tableAccessWrapper = tableAccessWrapper;
        this.commitTableClient = commitTableClient;
    }

    public SnapshotFilterImpl(TableAccessWrapper tableAccessWrapper) throws IOException {
        this(tableAccessWrapper, null);
    }

    public SnapshotFilterImpl(CommitTable.Client commitTableClient) throws IOException {
        this(null, commitTableClient);
    }

    void setTableAccessWrapper(TableAccessWrapper tableAccessWrapper) {
        this.tableAccessWrapper = tableAccessWrapper;
    }

    /**
     * Check whether a cell was deleted using family deletion marker
     *
     * @param cell                The cell to check
     * @param transaction         Defines the current snapshot
     * @param familyDeletionCache Accumulates the family deletion markers to identify cells that deleted with a higher version
     * @param commitCache         Holds shadow cells information
     * @return Whether the cell was deleted
     */
    private boolean checkFamilyDeletionCache(Cell cell, HBaseTransaction transaction, Map<String, List<Cell>> familyDeletionCache, Map<Long, Long> commitCache) throws IOException {
        List<Cell> familyDeletionCells = familyDeletionCache.get(Bytes.toString((cell.getRow())));
        if (familyDeletionCells != null) {
            for(Cell familyDeletionCell : familyDeletionCells) {
                String family = Bytes.toString(cell.getFamily());
                String familyDeletion = Bytes.toString(familyDeletionCell.getFamily());
                if (family.equals(familyDeletion)) {
                    Optional<Long> familyDeletionCommitTimestamp = getCommitTimestamp(familyDeletionCell, transaction, commitCache);
                    if (familyDeletionCommitTimestamp.isPresent() && familyDeletionCommitTimestamp.get() >= cell.getTimestamp()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void healShadowCell(Cell cell, long commitTimestamp) {
        Put put = new Put(CellUtil.cloneRow(cell));
        byte[] family = CellUtil.cloneFamily(cell);
        byte[] shadowCellQualifier = CellUtils.addShadowCellSuffix(cell.getQualifierArray(),
                                                                   cell.getQualifierOffset(),
                                                                   cell.getQualifierLength());
        put.add(family, shadowCellQualifier, cell.getTimestamp(), Bytes.toBytes(commitTimestamp));
        try {
            tableAccessWrapper.put(put);
        } catch (IOException e) {
            LOG.warn("Failed healing shadow cell for kv {}", cell, e);
        }
    }

    /**
     * Check if the transaction commit data is in the shadow cell
     * @param cellStartTimestamp
     *            the transaction start timestamp
     *        locator
     *            the timestamp locator
     * @throws IOException
     */
    @Override
    public Optional<CommitTimestamp> readCommitTimestampFromShadowCell(long cellStartTimestamp, CommitTimestampLocator locator)
            throws IOException
    {

        Optional<CommitTimestamp> commitTS = Optional.absent();

        Optional<Long> commitTimestamp = locator.readCommitTimestampFromShadowCell(cellStartTimestamp);
        if (commitTimestamp.isPresent()) {
            commitTS = Optional.of(new CommitTimestamp(SHADOW_CELL, commitTimestamp.get(), true)); // Valid commit TS
        }

        return commitTS;
    }

    /**
     * This function returns the commit timestamp for a particular cell if the transaction was already committed in
     * the system. In case the transaction was not committed and the cell was written by transaction initialized by a
     * previous TSO server, an invalidation try occurs.
     * Otherwise the function returns a value that indicates that the commit timestamp was not found.
     * @param cellStartTimestamp
     *          start timestamp of the cell to locate the commit timestamp for.
     * @param epoch
     *          the epoch of the TSO server the current tso client is working with.
     * @param locator
     *          a locator to find the commit timestamp in the system.
     * @return the commit timestamp joint with the location where it was found
     *         or an object indicating that it was not found in the system
     * @throws IOException  in case of any I/O issues
     */
    @Override
    public CommitTimestamp locateCellCommitTimestamp(long cellStartTimestamp, long epoch,
                                                     CommitTimestampLocator locator) throws IOException {

        try {
            // 1) First check the cache
            Optional<Long> commitTimestamp = locator.readCommitTimestampFromCache(cellStartTimestamp);
            if (commitTimestamp.isPresent()) { // Valid commit timestamp
                return new CommitTimestamp(CACHE, commitTimestamp.get(), true);
            }

            // 2) Then check the commit table
            // If the data was written at a previous epoch, check whether the transaction was invalidated
            Optional<CommitTimestamp> commitTimeStamp = commitTableClient.getCommitTimestamp(cellStartTimestamp).get();
            if (commitTimeStamp.isPresent()) {
                return commitTimeStamp.get();
            }

            // 3) Read from shadow cell
            commitTimeStamp = readCommitTimestampFromShadowCell(cellStartTimestamp, locator);
            if (commitTimeStamp.isPresent()) {
                return commitTimeStamp.get();
            }

            // 4) Check the epoch and invalidate the entry
            // if the data was written by a transaction from a previous epoch (previous TSO)
            if (cellStartTimestamp < epoch) {
                boolean invalidated = commitTableClient.tryInvalidateTransaction(cellStartTimestamp).get();
                if (invalidated) { // Invalid commit timestamp
                    return new CommitTimestamp(COMMIT_TABLE, CommitTable.INVALID_TRANSACTION_MARKER, false);
                }
            }

            // 5) We did not manage to invalidate the transactions then check the commit table
            commitTimeStamp = commitTableClient.getCommitTimestamp(cellStartTimestamp).get();
            if (commitTimeStamp.isPresent()) {
                return commitTimeStamp.get();
            }

            // 6) Read from shadow cell
            commitTimeStamp = readCommitTimestampFromShadowCell(cellStartTimestamp, locator);
            if (commitTimeStamp.isPresent()) {
                return commitTimeStamp.get();
            }

            // *) Otherwise return not found
            return new CommitTimestamp(NOT_PRESENT, -1L /** TODO Check if we should return this */, true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while finding commit timestamp", e);
        } catch (ExecutionException e) {
            throw new IOException("Problem finding commit timestamp", e);
        }

    }

    private Optional<Long> tryToLocateCellCommitTimestamp(long epoch,
            Cell cell,
            Map<Long, Long> commitCache)
                    throws IOException {

        CommitTimestamp tentativeCommitTimestamp =
                locateCellCommitTimestamp(
                        cell.getTimestamp(),
                        epoch,
                        new CommitTimestampLocatorImpl(
                                new HBaseCellId(null,
                                        CellUtil.cloneRow(cell),
                                        CellUtil.cloneFamily(cell),
                                        CellUtil.cloneQualifier(cell),
                                        cell.getTimestamp()),
                                        commitCache,
                                        tableAccessWrapper));

        // If transaction that added the cell was invalidated
        if (!tentativeCommitTimestamp.isValid()) {
            return Optional.absent();
        }

        switch (tentativeCommitTimestamp.getLocation()) {
        case COMMIT_TABLE:
            // If the commit timestamp is found in the persisted commit table,
            // that means the writing process of the shadow cell in the post
            // commit phase of the client probably failed, so we heal the shadow
            // cell with the right commit timestamp for avoiding further reads to
            // hit the storage
            healShadowCell(cell, tentativeCommitTimestamp.getValue());
            return Optional.of(tentativeCommitTimestamp.getValue());
        case CACHE:
        case SHADOW_CELL:
            return Optional.of(tentativeCommitTimestamp.getValue());
        case NOT_PRESENT:
            return Optional.absent();
        default:
            assert (false);
            return Optional.absent();
        }
    }
    
    
    private Optional<Long> getCommitTimestamp(Cell kv, HBaseTransaction transaction, Map<Long, Long> commitCache)
            throws IOException {

        long startTimestamp = transaction.getStartTimestamp();

        if (kv.getTimestamp() == startTimestamp) {
            return Optional.of(startTimestamp);
        }

        if (commitTableClient == null) {
            assert (transaction.getTransactionManager() != null);
            commitTableClient = transaction.getTransactionManager().getCommitTableClient();
        }

        return tryToLocateCellCommitTimestamp(transaction.getEpoch(), kv,
                commitCache);
    }
    
    private Map<Long, Long> buildCommitCache(List<Cell> rawCells) {

        Map<Long, Long> commitCache = new HashMap<>();

        for (Cell cell : rawCells) {
            if (CellUtils.isShadowCell(cell)) {
                commitCache.put(cell.getTimestamp(), Bytes.toLong(CellUtil.cloneValue(cell)));
            }
        }

        return commitCache;
    }

    private void buildFamilyDeletionCache(List<Cell> rawCells, Map<String, List<Cell>> familyDeletionCache) {

        for (Cell cell : rawCells) {
            if (CellUtil.matchingQualifier(cell, CellUtils.FAMILY_DELETE_QUALIFIER) &&
                    CellUtil.matchingValue(cell, HConstants.EMPTY_BYTE_ARRAY)) {

                String row = Bytes.toString(cell.getRow());
                List<Cell> cells = familyDeletionCache.get(row);
                if (cells == null) {
                    cells = new ArrayList<>();
                    familyDeletionCache.put(row, cells);
                }

                cells.add(cell);
            }
        }

    }

    private boolean isCellInTransaction(Cell kv, HBaseTransaction transaction, Map<Long, Long> commitCache) {

        long startTimestamp = transaction.getStartTimestamp();
        long readTimestamp = transaction.getReadTimestamp();

        // A cell was written by a transaction if its timestamp is larger than its startTimestamp and smaller or equal to its readTimestamp.
        // There also might be a case where the cell was written by the transaction and its timestamp equals to its writeTimestamp, however,
        // this case occurs after checkpoint and in this case we do not want to read this data.
        if (kv.getTimestamp() >= startTimestamp && kv.getTimestamp() <= readTimestamp) {
            return true;
        }

        return false;
    }

    private boolean isCellInSnapshot(Cell kv, HBaseTransaction transaction, Map<Long, Long> commitCache)
        throws IOException {

        Optional<Long> commitTimestamp = getCommitTimestamp(kv, transaction, commitCache);

        return commitTimestamp.isPresent() && commitTimestamp.get() < transaction.getStartTimestamp();
    }

    private Get createPendingGet(Cell cell, int versionCount) throws IOException {

        Get pendingGet = new Get(CellUtil.cloneRow(cell));
        pendingGet.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell));
        pendingGet.addColumn(CellUtil.cloneFamily(cell), CellUtils.addShadowCellSuffix(cell.getQualifierArray(),
                                                                                       cell.getQualifierOffset(),
                                                                                       cell.getQualifierLength()));
        pendingGet.setMaxVersions(versionCount);
        pendingGet.setTimeRange(0, cell.getTimestamp());

        return pendingGet;
    }

    /**
     * Filters the raw results returned from HBase and returns only those belonging to the current snapshot, as defined
     * by the transaction object. If the raw results don't contain enough information for a particular qualifier, it
     * will request more versions from HBase.
     *
     * @param rawCells          Raw cells that we are going to filter
     * @param transaction       Defines the current snapshot
     * @param versionsToRequest Number of versions requested from hbase
     * @param familyDeletionCache Accumulates the family deletion markers to identify cells that deleted with a higher version
     * @return Filtered KVs belonging to the transaction snapshot
     */
    @Override
    public List<Cell> filterCellsForSnapshot(List<Cell> rawCells, HBaseTransaction transaction,
                                      int versionsToRequest, Map<String, List<Cell>> familyDeletionCache, Map<String,byte[]> attributeMap) throws IOException {

        assert (rawCells != null && transaction != null && versionsToRequest >= 1);

        List<Cell> keyValuesInSnapshot = new ArrayList<>();
        List<Get> pendingGetsList = new ArrayList<>();

        int numberOfVersionsToFetch = versionsToRequest * 2;
        if (numberOfVersionsToFetch < 1) {
            numberOfVersionsToFetch = versionsToRequest;
        }

        Map<Long, Long> commitCache = buildCommitCache(rawCells);
        buildFamilyDeletionCache(rawCells, familyDeletionCache);

        for (Collection<Cell> columnCells : groupCellsByColumnFilteringShadowCellsAndFamilyDeletion(rawCells)) {
            boolean snapshotValueFound = false;
            Cell oldestCell = null;
            for (Cell cell : columnCells) {
                snapshotValueFound = checkFamilyDeletionCache(cell, transaction, familyDeletionCache, commitCache);

                if (snapshotValueFound == true) {
                    if (transaction.getVisibilityLevel() == VisibilityLevel.SNAPSHOT_ALL) {
                        snapshotValueFound = false;
                    } else {
                        break;
                    }
                }

                if (isCellInTransaction(cell, transaction, commitCache) ||
                    isCellInSnapshot(cell, transaction, commitCache)) {
                    if (!CellUtil.matchingValue(cell, CellUtils.DELETE_TOMBSTONE)) {
                        keyValuesInSnapshot.add(cell);
                    }

                    // We can finish looking for additional results in two cases:
                    // 1. if we found a result and we are not in SNAPSHOT_ALL mode.
                    // 2. if we found a result that was not written by the current transaction.
                    if (transaction.getVisibilityLevel() != VisibilityLevel.SNAPSHOT_ALL ||
                        !isCellInTransaction(cell, transaction, commitCache)) {
                        snapshotValueFound = true;
                        break;
                    }
                }
                oldestCell = cell;
            }
            if (!snapshotValueFound) {
                assert (oldestCell != null);
                Get pendingGet = createPendingGet(oldestCell, numberOfVersionsToFetch);
                for (Map.Entry<String,byte[]> entry : attributeMap.entrySet()) {
                    pendingGet.setAttribute(entry.getKey(), entry.getValue());
                }
                pendingGetsList.add(pendingGet);
            }
        }

        if (!pendingGetsList.isEmpty()) {
            Result[] pendingGetsResults = tableAccessWrapper.get(pendingGetsList);
            for (Result pendingGetResult : pendingGetsResults) {
                if (!pendingGetResult.isEmpty()) {
                    keyValuesInSnapshot.addAll(
                        filterCellsForSnapshot(pendingGetResult.listCells(), transaction, numberOfVersionsToFetch, familyDeletionCache, attributeMap));
                }
            }
        }

        Collections.sort(keyValuesInSnapshot, KeyValue.COMPARATOR);

        return keyValuesInSnapshot;
    }

    @Override
    public Result get(TTable ttable, Get get, HBaseTransaction transaction) throws IOException {
        Result result = tableAccessWrapper.get(get);

        List<Cell> filteredKeyValues = Collections.emptyList();
        if (!result.isEmpty()) {
            filteredKeyValues = ttable.filterCellsForSnapshot(result.listCells(), transaction, get.getMaxVersions(), new HashMap<String, List<Cell>>(), get.getAttributesMap());
        }

        return Result.create(filteredKeyValues);
    }

    @Override
    public ResultScanner getScanner(TTable ttable, Scan scan, HBaseTransaction transaction) throws IOException {

        return ttable.new TransactionalClientScanner(transaction, scan, 1);

    }

    @Override
    public boolean isCommitted(HBaseCellId hBaseCellId, long epoch) throws TransactionException {
        try {
            long timestamp = hBaseCellId.getTimestamp() - (hBaseCellId.getTimestamp() % AbstractTransactionManager.MAX_CHECKPOINTS_PER_TXN);
            CommitTimestamp tentativeCommitTimestamp =
                    locateCellCommitTimestamp(timestamp, epoch,
                                              new CommitTimestampLocatorImpl(hBaseCellId, Maps.<Long, Long>newHashMap(), tableAccessWrapper));

            // If transaction that added the cell was invalidated
            if (!tentativeCommitTimestamp.isValid()) {
                return false;
            }

            switch (tentativeCommitTimestamp.getLocation()) {
                case COMMIT_TABLE:
                case SHADOW_CELL:
                    return true;
                case NOT_PRESENT:
                    return false;
                case CACHE: // cache was empty
                default:
                    return false;
            }
        } catch (IOException e) {
            throw new TransactionException("Failure while checking if a transaction was committed", e);
        }
    }

    static ImmutableList<Collection<Cell>> groupCellsByColumnFilteringShadowCellsAndFamilyDeletion(List<Cell> rawCells) {

        Predicate<Cell> shadowCellAndFamilyDeletionFilter = new Predicate<Cell>() {

            @Override
            public boolean apply(Cell cell) {
                boolean familyDeletionMarkerCondition = CellUtil.matchingQualifier(cell, CellUtils.FAMILY_DELETE_QUALIFIER) &&
                                                        CellUtil.matchingValue(cell, HConstants.EMPTY_BYTE_ARRAY);

                return cell != null && !CellUtils.isShadowCell(cell) && !familyDeletionMarkerCondition;
            }

        };

        Function<Cell, ColumnWrapper> cellToColumnWrapper = new Function<Cell, ColumnWrapper>() {

            @Override
            public ColumnWrapper apply(Cell cell) {
                return new ColumnWrapper(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell));
            }

        };

        return Multimaps.index(Iterables.filter(rawCells, shadowCellAndFamilyDeletionFilter), cellToColumnWrapper)
            .asMap().values()
            .asList();
    }

}
