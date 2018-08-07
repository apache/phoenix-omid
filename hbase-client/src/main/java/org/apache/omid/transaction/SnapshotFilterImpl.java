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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.CommitTable.CommitTimestamp;
import org.apache.omid.transaction.AbstractTransaction.VisibilityLevel;
import org.apache.omid.transaction.HBaseTransactionManager.CommitTimestampLocatorImpl;
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

    public CommitTable.Client getCommitTableClient() {
        return commitTableClient;
    }

    private CommitTable.Client commitTableClient;

    public TableAccessWrapper getTableAccessWrapper() {
        return tableAccessWrapper;
    }

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

    void setCommitTableClient(CommitTable.Client commitTableClient) {
        this.commitTableClient = commitTableClient;
    }

    private String getRowFamilyString(Cell cell) {
        return Bytes.toString((CellUtil.cloneRow(cell))) + ":" + Bytes.toString(CellUtil.cloneFamily(cell));
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
    private boolean checkFamilyDeletionCache(Cell cell, HBaseTransaction transaction, Map<String, Long> familyDeletionCache, Map<Long, Long> commitCache) throws IOException {
        String key = getRowFamilyString(cell);
        Long familyDeletionCommitTimestamp = familyDeletionCache.get(key);
        if (familyDeletionCommitTimestamp != null && familyDeletionCommitTimestamp >= cell.getTimestamp()) {
            return true;
        }
        return false;
    }

    private void healShadowCell(Cell cell, long commitTimestamp) {
        Put put = new Put(CellUtil.cloneRow(cell));
        byte[] family = CellUtil.cloneFamily(cell);
        byte[] shadowCellQualifier = CellUtils.addShadowCellSuffixPrefix(cell.getQualifierArray(),
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

    public Optional<Long> tryToLocateCellCommitTimestamp(long epoch,
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


    private void buildFamilyDeletionCache(HBaseTransaction transaction, List<Cell> rawCells, Map<String, Long> familyDeletionCache, Map<Long, Long> commitCache, Map<String,byte[]> attributeMap) throws IOException {
        for (Cell cell : rawCells) {
            if (CellUtils.isFamilyDeleteCell(cell)) {
                String key = getRowFamilyString(cell);

                if (familyDeletionCache.containsKey(key))
                    return;

                Optional<Long> commitTimeStamp = getTSIfInTransaction(cell, transaction);

                if (!commitTimeStamp.isPresent()) {
                    commitTimeStamp = getTSIfInSnapshot(cell, transaction, commitCache);
                }

                if (commitTimeStamp.isPresent()) {
                    familyDeletionCache.put(key, commitTimeStamp.get());
                } else {
                    Cell lastCell = cell;
                    Map<Long, Long> cmtCache;
                    boolean foundCommittedFamilyDeletion = false;
                    while (!foundCommittedFamilyDeletion) {

                        Get g = createPendingGet(lastCell, 3);

                        Result result = tableAccessWrapper.get(g);
                        List<Cell> resultCells = result.listCells();
                        if (resultCells == null) {
                            break;
                        }

                        cmtCache = buildCommitCache(resultCells);
                        for (Cell c : resultCells) {
                            if (CellUtils.isFamilyDeleteCell(c)) {
                                    commitTimeStamp = getTSIfInSnapshot(c, transaction, cmtCache);
                                    if (commitTimeStamp.isPresent()) {
                                        familyDeletionCache.put(key, commitTimeStamp.get());
                                        foundCommittedFamilyDeletion = true;
                                        break;
                                    }
                                    lastCell = c;
                            }
                        }
                    }
                }
            }
        }
    }


    public Optional<Long> getTSIfInTransaction(Cell kv, HBaseTransaction transaction) {
        long startTimestamp = transaction.getStartTimestamp();
        long readTimestamp = transaction.getReadTimestamp();

        // A cell was written by a transaction if its timestamp is larger than its startTimestamp and smaller or equal to its readTimestamp.
        // There also might be a case where the cell was written by the transaction and its timestamp equals to its writeTimestamp, however,
        // this case occurs after checkpoint and in this case we do not want to read this data.
        if (kv.getTimestamp() >= startTimestamp && kv.getTimestamp() <= readTimestamp) {
            return Optional.of(kv.getTimestamp());
        }

        return Optional.absent();
    }


    public Optional<Long> getTSIfInSnapshot(Cell kv, HBaseTransaction transaction, Map<Long, Long> commitCache)
        throws IOException {

        Optional<Long> commitTimestamp = getCommitTimestamp(kv, transaction, commitCache);

        if (commitTimestamp.isPresent() && commitTimestamp.get() < transaction.getStartTimestamp())
            return commitTimestamp;

        return Optional.absent();
    }

    private Get createPendingGet(Cell cell, int versionCount) throws IOException {

        Get pendingGet = new Get(CellUtil.cloneRow(cell));
        pendingGet.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell));
        pendingGet.addColumn(CellUtil.cloneFamily(cell), CellUtils.addShadowCellSuffixPrefix(cell.getQualifierArray(),
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
                                      int versionsToRequest, Map<String, Long> familyDeletionCache, Map<String,byte[]> attributeMap) throws IOException {

        assert (rawCells != null && transaction != null && versionsToRequest >= 1);

        List<Cell> keyValuesInSnapshot = new ArrayList<>();
        List<Get> pendingGetsList = new ArrayList<>();

        int numberOfVersionsToFetch = versionsToRequest * 2;
        if (numberOfVersionsToFetch < 1) {
            numberOfVersionsToFetch = versionsToRequest;
        }

        Map<Long, Long> commitCache = buildCommitCache(rawCells);
        buildFamilyDeletionCache(transaction, rawCells, familyDeletionCache, commitCache, attributeMap);

        ImmutableList<Collection<Cell>> filteredCells;
        if (transaction.getVisibilityLevel() == VisibilityLevel.SNAPSHOT_ALL) {
            filteredCells = groupCellsByColumnFilteringShadowCells(rawCells);
        } else {
            filteredCells = groupCellsByColumnFilteringShadowCellsAndFamilyDeletion(rawCells);
        }

        for (Collection<Cell> columnCells : filteredCells) {
            boolean snapshotValueFound = false;
            Cell oldestCell = null;
            for (Cell cell : columnCells) {
                oldestCell = cell;
                if (getTSIfInTransaction(cell, transaction).isPresent() ||
                        getTSIfInSnapshot(cell, transaction, commitCache).isPresent()) {

                    if (transaction.getVisibilityLevel() == VisibilityLevel.SNAPSHOT_ALL) {
                        keyValuesInSnapshot.add(cell);
                        if (getTSIfInTransaction(cell, transaction).isPresent()) {
                            snapshotValueFound = false;
                            continue;
                        } else {
                            snapshotValueFound = true;
                            break;
                        }
                    } else {
                        if (!checkFamilyDeletionCache(cell, transaction, familyDeletionCache, commitCache) &&
                                !CellUtils.isTombstone(cell)) {
                            keyValuesInSnapshot.add(cell);
                        }
                        snapshotValueFound = true;
                        break;

                    }
                }
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
    public Result get(Get get, HBaseTransaction transaction) throws IOException {
        Result result = tableAccessWrapper.get(get);

        List<Cell> filteredKeyValues = Collections.emptyList();
        if (!result.isEmpty()) {
            filteredKeyValues = filterCellsForSnapshot(result.listCells(), transaction, get.getMaxVersions(), new HashMap<String, Long>(), get.getAttributesMap());
        }

        return Result.create(filteredKeyValues);
    }

    @Override
    public ResultScanner getScanner(Scan scan, HBaseTransaction transaction) throws IOException {

        return new TransactionalClientScanner(transaction, scan, 1);

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
                boolean familyDeletionMarkerCondition = CellUtils.isFamilyDeleteCell(cell);

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


    static ImmutableList<Collection<Cell>> groupCellsByColumnFilteringShadowCells(List<Cell> rawCells) {

        Predicate<Cell> shadowCellFilter = new Predicate<Cell>() {
            @Override
            public boolean apply(Cell cell) {
                return cell != null && !CellUtils.isShadowCell(cell);
            }
        };

        Function<Cell, ColumnWrapper> cellToColumnWrapper = new Function<Cell, ColumnWrapper>() {

            @Override
            public ColumnWrapper apply(Cell cell) {
                return new ColumnWrapper(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell));
            }

        };

        return Multimaps.index(Iterables.filter(rawCells, shadowCellFilter), cellToColumnWrapper)
                .asMap().values()
                .asList();
    }


    public class TransactionalClientScanner implements ResultScanner {

        private HBaseTransaction state;
        private ResultScanner innerScanner;
        private int maxVersions;
        Map<String, Long> familyDeletionCache;
        private Map<String,byte[]> attributeMap;

        TransactionalClientScanner(HBaseTransaction state, Scan scan, int maxVersions)
                throws IOException {
            if (scan.hasFilter()) {
                LOG.warn("Client scanner with filter will return un expected results. Use Coprocessor scanning");
            }
            this.state = state;
            this.innerScanner = tableAccessWrapper.getScanner(scan);
            this.maxVersions = maxVersions;
            this.familyDeletionCache = new HashMap<String, Long>();
            this.attributeMap = scan.getAttributesMap();
        }


        @Override
        public Result next() throws IOException {
            List<Cell> filteredResult = Collections.emptyList();
            while (filteredResult.isEmpty()) {
                Result result = innerScanner.next();
                if (result == null) {
                    return null;
                }
                if (!result.isEmpty()) {
                    filteredResult = filterCellsForSnapshot(result.listCells(), state, maxVersions, familyDeletionCache, attributeMap);
                }
            }
            return Result.create(filteredResult);
        }

        // In principle no need to override, copied from super.next(int) to make
        // sure it works even if super.next(int)
        // changes its implementation
        @Override
        public Result[] next(int nbRows) throws IOException {
            // Collect values to be returned here
            ArrayList<Result> resultSets = new ArrayList<>(nbRows);
            for (int i = 0; i < nbRows; i++) {
                Result next = next();
                if (next != null) {
                    resultSets.add(next);
                } else {
                    break;
                }
            }
            return resultSets.toArray(new Result[resultSets.size()]);
        }

        @Override
        public void close() {
            innerScanner.close();
        }

        @Override
        public Iterator<Result> iterator() {
            return new ResultIterator(this);
        }

        // ------------------------------------------------------------------------------------------------------------
        // --------------------------------- Helper class for TransactionalClientScanner ------------------------------
        // ------------------------------------------------------------------------------------------------------------

        class ResultIterator implements Iterator<Result> {

            TransactionalClientScanner scanner;
            Result currentResult;

            ResultIterator(TransactionalClientScanner scanner) {
                try {
                    this.scanner = scanner;
                    currentResult = scanner.next();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean hasNext() {
                return currentResult != null && !currentResult.isEmpty();
            }

            @Override
            public Result next() {
                try {
                    Result result = currentResult;
                    currentResult = scanner.next();
                    return result;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void remove() {
                throw new RuntimeException("Not implemented");
            }

        }

    }


}
