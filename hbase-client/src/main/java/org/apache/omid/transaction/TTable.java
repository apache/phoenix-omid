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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.tso.client.OmidClientConfiguration.ConflictDetectionLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provides transactional methods for accessing and modifying a given snapshot of data identified by an opaque {@link
 * Transaction} object. It mimics the behavior in {@link org.apache.hadoop.hbase.client.Table}
 */
public class TTable implements Closeable {

    private static Logger LOG = LoggerFactory.getLogger(TTable.class);

    private Table table;

    private SnapshotFilter snapshotFilter;

    private boolean serverSideFilter;
    
    private final List<Mutation> mutations;
    
    private boolean autoFlush = true;
    
    private final boolean conflictFree;
    
    // ----------------------------------------------------------------------------------------------------------------
    // Construction
    // ----------------------------------------------------------------------------------------------------------------

    public TTable(Connection connection, byte[] tableName) throws IOException {
        this(connection.getTable(TableName.valueOf(tableName)), false);
    }

    public TTable(Connection connection, byte[] tableName, CommitTable.Client commitTableClient) throws IOException {
        this(connection.getTable(TableName.valueOf(tableName)), commitTableClient, false);
    }

    public TTable(Connection connection, String tableName) throws IOException {
        this(connection.getTable(TableName.valueOf(tableName)), false);
    }

    public TTable(Connection connection, String tableName, CommitTable.Client commitTableClient) throws IOException {
        this(connection.getTable(TableName.valueOf(tableName)), commitTableClient, false);
    }

    public TTable(Table hTable) throws IOException {
        this(hTable, hTable.getConfiguration().getBoolean("omid.server.side.filter", false), false);
    }

    public TTable(Connection connection, byte[] tableName, boolean conflictFree) throws IOException {
        this(connection.getTable(TableName.valueOf(tableName)), conflictFree);
    }

    public TTable(Connection connection, byte[] tableName, CommitTable.Client commitTableClient, boolean conflictFree) throws IOException {
        this(connection.getTable(TableName.valueOf(tableName)), commitTableClient, conflictFree);
    }

    public TTable(Connection connection, String tableName, boolean conflictFree) throws IOException {
        this(connection.getTable(TableName.valueOf(tableName)), conflictFree);
    }

    public TTable(Connection connection, String tableName, CommitTable.Client commitTableClient, boolean conflictFree) throws IOException {
        this(connection.getTable(TableName.valueOf(tableName)), commitTableClient, conflictFree);
    }

    public TTable(Table hTable, boolean conflictFree) throws IOException {
        this(hTable, hTable.getConfiguration().getBoolean("omid.server.side.filter", false), conflictFree);
    }

    public TTable(Table hTable, SnapshotFilter snapshotFilter) throws IOException {
        this(hTable, snapshotFilter, false);
    }

    public TTable(Table hTable, CommitTable.Client commitTableClient) throws IOException {
        this(hTable, commitTableClient, false);
    }

    public TTable(Table hTable, boolean serverSideFilter, boolean conflictFree) throws IOException {
        this.table = hTable;
        this.conflictFree = conflictFree;
        this.mutations = new ArrayList<Mutation>();
        this.serverSideFilter = serverSideFilter;
        this.snapshotFilter = (serverSideFilter) ?  new AttributeSetSnapshotFilter(hTable) :
                new SnapshotFilterImpl(new HTableAccessWrapper(hTable, hTable));
    }

    public TTable(Table hTable, SnapshotFilter snapshotFilter, boolean conflictFree) throws IOException {
        this.table = hTable;
        this.conflictFree = conflictFree;
        this.mutations = new ArrayList<Mutation>();
        this.snapshotFilter = snapshotFilter;
    }

    public TTable(Table hTable, CommitTable.Client commitTableClient, boolean conflictFree) throws IOException {
        this.table = hTable;
        this.conflictFree = conflictFree;
        this.mutations = new ArrayList<Mutation>();
        this.serverSideFilter = table.getConfiguration().getBoolean("omid.server.side.filter", false);
        this.snapshotFilter = (serverSideFilter) ?  new AttributeSetSnapshotFilter(hTable) :
                new SnapshotFilterImpl(new HTableAccessWrapper(hTable, hTable), commitTableClient);
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Closeable implementation
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * Releases any resources held or pending changes in internal buffers.
     *
     * @throws IOException if a remote or network exception occurs.
     */
    @Override
    public void close() throws IOException {
        table.close();
        try {
            snapshotFilter.close();
        } catch (Exception e) {
            LOG.warn("Failed to close TTable resources.");
            e.printStackTrace();
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Transactional operations
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * Transactional version of {@link Table#get(Get get)}
     *
     * @param get an instance of Get
     * @param tx  an instance of transaction to be used
     * @return Result an instance of Result
     * @throws IOException if a remote or network exception occurs.
     */
    public Result get(Transaction tx, final Get get) throws IOException {

        throwExceptionIfOpSetsTimerange(get);

        flushCommits();

        HBaseTransaction transaction = enforceHBaseTransactionAsParam(tx);

        final long readTimestamp = transaction.getReadTimestamp();
        final Get tsget = new Get(get.getRow()).setFilter(get.getFilter());
        propagateAttributes(get, tsget);
        TimeRange timeRange = get.getTimeRange();
        long startTime = timeRange.getMin();
        long endTime = Math.min(timeRange.getMax(), readTimestamp + 1);
        tsget.setTimeRange(startTime, endTime).readVersions(1);
        Map<byte[], NavigableSet<byte[]>> kvs = get.getFamilyMap();
        for (Map.Entry<byte[], NavigableSet<byte[]>> entry : kvs.entrySet()) {
            byte[] family = entry.getKey();
            NavigableSet<byte[]> qualifiers = entry.getValue();
            if (qualifiers == null || qualifiers.isEmpty()) {
                tsget.addFamily(family);
            } else {
                for (byte[] qualifier : qualifiers) {
                    tsget.addColumn(family, qualifier);
                    tsget.addColumn(family, CellUtils.addShadowCellSuffixPrefix(qualifier));
                }
                tsget.addColumn(family, CellUtils.FAMILY_DELETE_QUALIFIER);
                tsget.addColumn(family, CellUtils.addShadowCellSuffixPrefix(CellUtils.FAMILY_DELETE_QUALIFIER));
            }
        }
        LOG.trace("Initial Get = {}", tsget);

        return snapshotFilter.get(tsget, transaction);
    }

    static private void propagateAttributes(OperationWithAttributes from, OperationWithAttributes to) {
        Map<String,byte[]> attributeMap = from.getAttributesMap();

        for (Map.Entry<String,byte[]> entry : attributeMap.entrySet()) {
            to.setAttribute(entry.getKey(), entry.getValue());
        }
    }

    private void familyQualifierBasedDeletion(HBaseTransaction tx, Put deleteP, Get deleteG) throws IOException {
        Result result = this.get(tx, deleteG);
        if (!result.isEmpty()) {
            for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entryF : result.getMap()
                    .entrySet()) {
                byte[] family = entryF.getKey();
                for (Entry<byte[], NavigableMap<Long, byte[]>> entryQ : entryF.getValue().entrySet()) {
                    byte[] qualifier = entryQ.getKey();
                    addWriteSetElement(tx, new HBaseCellId(this, deleteP.getRow(), family, qualifier,
                            tx.getWriteTimestamp()));
                }
                deleteP.addColumn(family, CellUtils.FAMILY_DELETE_QUALIFIER, tx.getWriteTimestamp(),
                        CellUtils.DELETE_TOMBSTONE);
                addWriteSetElement(tx, new HBaseCellId(this, deleteP.getRow(), family, CellUtils.FAMILY_DELETE_QUALIFIER,
                                                tx.getWriteTimestamp()));
            }
        }
    }

    private void  familyQualifierBasedDeletionWithOutRead(HBaseTransaction tx, Put deleteP, Get deleteG) {
        Set<byte[]> fset = deleteG.getFamilyMap().keySet();

        for (byte[] family : fset) {
            deleteP.addColumn(family, CellUtils.FAMILY_DELETE_QUALIFIER, tx.getWriteTimestamp(),
                    CellUtils.DELETE_TOMBSTONE);
            addWriteSetElement(tx, new HBaseCellId(this, deleteP.getRow(), family, CellUtils.FAMILY_DELETE_QUALIFIER,
                    tx.getWriteTimestamp()));

        }
    }

    /**
     * Transactional version of {@link Table#delete(Delete delete)}
     *
     * @param delete an instance of Delete
     * @param tx     an instance of transaction to be used
     * @throws IOException if a remote or network exception occurs.
     */
    public void delete(Transaction tx, Delete delete) throws IOException {
        Put deleteP = deleteInternal(tx, delete);
        if (!deleteP.isEmpty()) {
            addMutation(deleteP);
        }
    }
    
    private Put deleteInternal(Transaction tx, Delete delete) throws IOException {

        throwExceptionIfOpSetsTimerange(delete);

        HBaseTransaction transaction = enforceHBaseTransactionAsParam(tx);

        final long writeTimestamp = transaction.getWriteTimestamp();
        boolean deleteFamily = false;

        final Put deleteP = new Put(delete.getRow(), writeTimestamp);
        final Get deleteG = new Get(delete.getRow());
        propagateAttributes(delete, deleteP);
        propagateAttributes(delete, deleteG);
        Map<byte[], List<Cell>> fmap = delete.getFamilyCellMap();
        if (fmap.isEmpty()) {
            familyQualifierBasedDeletion(transaction, deleteP, deleteG);
        }

        for (List<Cell> cells : fmap.values()) {
            for (Cell cell : cells) {
                CellUtils.validateCell(cell, writeTimestamp);
                switch (cell.getType()) {
                    case DeleteColumn:
                        deleteP.addColumn(CellUtil.cloneFamily(cell),
                                    CellUtil.cloneQualifier(cell),
                                    writeTimestamp,
                                    CellUtils.DELETE_TOMBSTONE);
                        addWriteSetElement(transaction,
                            new HBaseCellId(this,
                                            delete.getRow(),
                                            CellUtil.cloneFamily(cell),
                                            CellUtil.cloneQualifier(cell),
                                            writeTimestamp));
                        break;
                    case DeleteFamily:
                        deleteG.addFamily(CellUtil.cloneFamily(cell));
                        deleteFamily = true;
                        break;
                    case Delete:
                        if (cell.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
                            deleteP.addColumn(CellUtil.cloneFamily(cell),
                                        CellUtil.cloneQualifier(cell),
                                        writeTimestamp,
                                        CellUtils.DELETE_TOMBSTONE);
                            addWriteSetElement(transaction,
                                new HBaseCellId(this,
                                                delete.getRow(),
                                                CellUtil.cloneFamily(cell),
                                                CellUtil.cloneQualifier(cell),
                                                writeTimestamp));
                            break;
                        } else {
                            throw new UnsupportedOperationException(
                                "Cannot delete specific versions on Snapshot Isolation.");
                        }
                    default:
                        break;
                }
            }
        }
        if (deleteFamily) {
            if (enforceHBaseTransactionManagerAsParam(transaction.getTransactionManager()).
                    getConflictDetectionLevel() == ConflictDetectionLevel.ROW) {
                familyQualifierBasedDeletionWithOutRead(transaction, deleteP, deleteG);
            } else {
                familyQualifierBasedDeletion(transaction, deleteP, deleteG);
            }
        }

        return deleteP;
    }

    /**
     * Transactional version of {@link Table#put(Put put)}
     *
     * @param put an instance of Put
     * @param tx  an instance of transaction to be used
     * @throws IOException if a remote or network exception occurs.
     */
    public void put(Transaction tx, Put put) throws IOException {
        put(tx, put, false);
    }


    /**
     * @param put an instance of Put
     * @param timestamp  timestamp to be used as cells version
     * @param commitTimestamp  timestamp to be used as commit timestamp
     */
    static public Put markPutAsCommitted(Put put, long timestamp, long commitTimestamp) {
        final Put tsput = new Put(put.getRow(), timestamp);
        propagateAttributes(put, tsput);

        Map<byte[], List<Cell>> kvs = put.getFamilyCellMap();
        for (List<Cell> kvl : kvs.values()) {
            for (Cell c : kvl) {
                KeyValue kv = KeyValueUtil.ensureKeyValue(c);
                Bytes.putLong(kv.getValueArray(), kv.getTimestampOffset(), timestamp);
                try {
                    tsput.add(kv);
                } catch (IOException e) {
                    // The existing Put has this Cell, so the cloned one
                    // will never throw an IOException when it's added.
                    throw new RuntimeException(e);
                }
                tsput.addColumn(CellUtil.cloneFamily(kv),
                        CellUtils.addShadowCellSuffixPrefix(CellUtil.cloneQualifier(kv), 0, CellUtil.cloneQualifier(kv).length),
                        kv.getTimestamp(),
                        Bytes.toBytes(commitTimestamp));
            }
        }

        return tsput;
    }


    /**
     * @param put an instance of Put
     * @param tx  an instance of transaction to be used
     * @param addShadowCell  denotes whether to add the shadow cell
     * @throws IOException if a remote or network exception occurs.
     */
    public void put(Transaction tx, Put put, boolean addShadowCell) throws IOException {
        Put tsput = putInternal(tx, put, addShadowCell);
        addMutation(tsput);
    }
    
    private Put putInternal(Transaction tx, Put put, boolean addShadowCell) throws IOException {

        throwExceptionIfOpSetsTimerange(put);

        HBaseTransaction transaction = enforceHBaseTransactionAsParam(tx);

        final long writeTimestamp = transaction.getWriteTimestamp();

        // create put with correct ts
        final Put tsput = new Put(put.getRow(), writeTimestamp);
        propagateAttributes(put, tsput);
        Map<byte[], List<Cell>> kvs = put.getFamilyCellMap();
        for (List<Cell> kvl : kvs.values()) {
            for (Cell c : kvl) {
                CellUtils.validateCell(c, writeTimestamp);
                // Reach into keyvalue to update timestamp.
                // It's not nice to reach into keyvalue internals,
                // but we want to avoid having to copy the whole thing
                KeyValue kv = KeyValueUtil.ensureKeyValue(c);
                Bytes.putLong(kv.getValueArray(), kv.getTimestampOffset(), writeTimestamp);
                tsput.add(kv);

                if (addShadowCell) {
                    tsput.addColumn(CellUtil.cloneFamily(kv),
                            CellUtils.addShadowCellSuffixPrefix(CellUtil.cloneQualifier(kv), 0, CellUtil.cloneQualifier(kv).length),
                            kv.getTimestamp(),
                            Bytes.toBytes(kv.getTimestamp()));
                } else {
                    HBaseCellId cellId = new HBaseCellId(this,
                            CellUtil.cloneRow(kv),
                            CellUtil.cloneFamily(kv),
                            CellUtil.cloneQualifier(kv),
                            kv.getTimestamp());

                    addWriteSetElement(transaction, cellId);
                }
            }
        }
        return tsput;
    }
    
    private void addWriteSetElement(HBaseTransaction transaction, HBaseCellId cellId) {
        if (conflictFree) {
            transaction.addConflictFreeWriteSetElement(cellId);
        } else {
            transaction.addWriteSetElement(cellId);
        }
        
    }

    private void addMutation(Mutation m) throws IOException {
        this.mutations.add(m);
        if (autoFlush) {
            flushCommits();
        }
    }
    
    private void addMutations(List<Mutation> mutations) throws IOException {
        this.mutations.addAll(mutations);
        if (autoFlush) {
            flushCommits();
        }
    }
    
    /**
     * Transactional version of {@link Table#getScanner(Scan scan)}
     *
     * @param scan an instance of Scan
     * @param tx   an instance of transaction to be used
     * @return ResultScanner an instance of ResultScanner
     * @throws IOException if a remote or network exception occurs.
     */
    public ResultScanner getScanner(Transaction tx, Scan scan) throws IOException {

        throwExceptionIfOpSetsTimerange(scan);
        flushCommits();
        HBaseTransaction transaction = enforceHBaseTransactionAsParam(tx);

        Scan tsscan = new Scan(scan);
        tsscan.readVersions(1);
        tsscan.setTimeRange(0, transaction.getReadTimestamp() + 1);
        propagateAttributes(scan, tsscan);
        Map<byte[], NavigableSet<byte[]>> kvs = scan.getFamilyMap();
        for (Map.Entry<byte[], NavigableSet<byte[]>> entry : kvs.entrySet()) {
            byte[] family = entry.getKey();
            NavigableSet<byte[]> qualifiers = entry.getValue();
            if (qualifiers == null) {
                continue;
            }
            for (byte[] qualifier : qualifiers) {
                tsscan.addColumn(family, CellUtils.addShadowCellSuffixPrefix(qualifier));
            }
            if (!qualifiers.isEmpty()) {
                tsscan.addColumn(entry.getKey(), CellUtils.FAMILY_DELETE_QUALIFIER);
            }
        }

        return snapshotFilter.getScanner(tsscan, transaction);
    }

    /**
     *
     * @return array of byte
     */
    public byte[] getTableName() {
        return table.getName().getName();
    }

    /**
     * Delegates to {@link Table#getConfiguration()}
     *
     * @return standard configuration object
     */
    public Configuration getConfiguration() {
        return table.getConfiguration();
    }

    /**
     * Delegates to {@link Table#getTableDescriptor()}
     *
     * This deprecated method is implemented for backwards compatibility reasons.
     * use {@link TTable#getDescriptor()}
     *
     * @return HTableDescriptor an instance of HTableDescriptor
     * @throws IOException if a remote or network exception occurs.
     */
    public HTableDescriptor getTableDescriptor() throws IOException {
        return table.getTableDescriptor();
    }

    /**
     * Delegates to {@link Table#getDescriptor()}
     *
     * @return TableDescriptor an instance of TableDescriptor
     * @throws IOException if a remote or network exception occurs.
     */
    public TableDescriptor getDescriptor() throws IOException {
        return table.getDescriptor();
    }

    /**
     * Transactional version of {@link Table#exists(Get get)}
     *
     * @param transaction an instance of transaction to be used
     * @param get         an instance of Get
     * @return true if cell exists
     * @throws IOException if a remote or network exception occurs.
     */
    public boolean exists(Transaction transaction, Get get) throws IOException {
        Result result = get(transaction, get);
        return !result.isEmpty();
    }

    /* TODO What should we do with this methods???
     * @Override public void batch(Transaction transaction, List<? extends Row>
     * actions, Object[] results) throws IOException, InterruptedException {}
     *
     * @Override public Object[] batch(Transaction transaction, List<? extends
     * Row> actions) throws IOException, InterruptedException {}
     *
     * @Override public <R> void batchCallback(Transaction transaction, List<?
     * extends Row> actions, Object[] results, Callback<R> callback) throws
     * IOException, InterruptedException {}
     *
     * @Override public <R> Object[] batchCallback(List<? extends Row> actions,
     * Callback<R> callback) throws IOException, InterruptedException {}
     */

    /**
     * Transactional version of {@link Table#get(List gets)}
     *
     * @param transaction an instance of transaction to be used
     * @param gets        list of Get instances
     * @return array of Results
     * @throws IOException if a remote or network exception occurs
     */
    public Result[] get(Transaction transaction, List<Get> gets) throws IOException {
        Result[] results = new Result[gets.size()];
        int i = 0;
        for (Get get : gets) {
            results[i++] = get(transaction, get);
        }
        return results;
    }

    /**
     * Transactional version of {@link Table#getScanner(byte[] family)}
     *
     * @param transaction an instance of transaction to be used
     * @param family      column family
     * @return an instance of ResultScanner
     * @throws IOException if a remote or network exception occurs
     */
    public ResultScanner getScanner(Transaction transaction, byte[] family) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(family);
        return getScanner(transaction, scan);
    }

    /**
     * Transactional version of {@link Table#getScanner(byte[] family, byte[] qualifier)}
     *
     * @param transaction an instance of transaction to be used
     * @param family      column family
     * @param qualifier   column name
     * @return an instance of ResultScanner
     * @throws IOException if a remote or network exception occurs
     */
    public ResultScanner getScanner(Transaction transaction, byte[] family, byte[] qualifier)
        throws IOException {
        Scan scan = new Scan();
        scan.addColumn(family, qualifier);
        return getScanner(transaction, scan);
    }

    /**
     * Transactional version of {@link Table#put(List puts)}
     *
     * @param transaction an instance of transaction to be used
     * @param puts        List of puts
     * @param addShadowCells  denotes whether to add the shadow cell
     * @throws IOException if a remote or network exception occurs
     */
    public void put(Transaction transaction, List<Put> puts, boolean addShadowCells) throws IOException {
        List<Mutation> mutations = new ArrayList<>(puts.size());
        for (Put put : puts) {
            mutations.add(putInternal(transaction, put, addShadowCells));
        }
        addMutations(mutations);
    }

    public void put(Transaction transaction, List<Put> puts) throws IOException {
        put(transaction, puts, false);
    }

    /**
     * Transactional version of Table#batch(List rows)
     *
     * @param transaction an instance of transaction to be used
     * @param rows        List of rows that must be instances of Put or Delete
     * @param addShadowCells  denotes whether to add the shadow cell
     * @throws IOException if a remote or network exception occurs
     */
    public void batch(Transaction transaction, List<? extends Row> rows, boolean addShadowCells) throws IOException {
        List<Mutation> mutations = new ArrayList<>(rows.size());
        for (Row row : rows) {
            if (row instanceof Put) {
                mutations.add(putInternal(transaction, (Put)row, addShadowCells));
            } else if (row instanceof Delete) {
                Put deleteP = deleteInternal(transaction, (Delete)row);
                if (!deleteP.isEmpty()) {
                    mutations.add(deleteP);
                }
            } else {
                throw new UnsupportedOperationException("Unsupported mutation: " + row);
            }
        }
        addMutations(mutations);
    }

    public void batch(Transaction transaction, List<? extends Row> rows) throws IOException {
        batch(transaction, rows, false);
    }

    /**
     * Transactional version of {@link Table#delete(List deletes)}
     *
     * @param transaction an instance of transaction to be used
     * @param deletes        List of deletes
     * @throws IOException if a remote or network exception occurs
     */
    public void delete(Transaction transaction, List<Delete> deletes) throws IOException {
        List<Mutation> mutations = new ArrayList<>(deletes.size());
        for (Delete delete : deletes) {
            Put deleteP = deleteInternal(transaction, delete);
            if (!deleteP.isEmpty()) {
                mutations.add(deleteP);
            }
        }
        addMutations(mutations);
    }

    /**
     * Provides access to the underliying Table in order to configure it or to perform unsafe (non-transactional)
     * operations. The latter would break the transactional guarantees of the whole system.
     *
     * @return The underlying Table object
     */
    public Table getHTable() {
        return table;
    }

    public void setAutoFlush(boolean autoFlush) throws IOException {
        this.autoFlush = autoFlush;
        flushCommits();
    }

    public boolean isAutoFlush() {
        return autoFlush;
    }

    public void flushCommits() throws IOException {
        try {
            if (this.mutations.size() > 0) {
                table.batch(this.mutations, new Object[mutations.size()]);
            }
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new RuntimeException(e);
        } finally {
            this.mutations.clear();
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------

    private void throwExceptionIfOpSetsTimerange(Get getOperation) {
        TimeRange tr = getOperation.getTimeRange();
        checkTimerangeIsSetToDefaultValuesOrThrowException(tr);
    }

    private void throwExceptionIfOpSetsTimerange(Scan scanOperation) {
        TimeRange tr = scanOperation.getTimeRange();
        checkTimerangeIsSetToDefaultValuesOrThrowException(tr);
    }

    private void checkTimerangeIsSetToDefaultValuesOrThrowException(TimeRange tr) {
        if (tr.getMin() != 0L || tr.getMax() != Long.MAX_VALUE) {
            throw new IllegalArgumentException(
                "Timestamp/timerange not allowed in transactional user operations");
        }
    }

    private void throwExceptionIfOpSetsTimerange(Mutation userOperation) {
        if (userOperation.getTimestamp() != HConstants.LATEST_TIMESTAMP) {
            throw new IllegalArgumentException(
                "Timestamp not allowed in transactional user operations");
        }
    }

    private HBaseTransaction enforceHBaseTransactionAsParam(Transaction tx) {
        if (tx instanceof HBaseTransaction) {
            return (HBaseTransaction) tx;
        } else {
            throw new IllegalArgumentException(
                String.format("The transaction object passed %s is not an instance of HBaseTransaction",
                              tx.getClass().getName()));
        }
    }

    private HBaseTransactionManager enforceHBaseTransactionManagerAsParam(TransactionManager tm) {
        if (tm instanceof HBaseTransactionManager) {
            return (HBaseTransactionManager) tm;
        } else {
            throw new IllegalArgumentException(
                String.format("The transaction manager object passed %s is not an instance of HBaseTransactionManager ",
                              tm.getClass().getName()));
        }
    }
}
