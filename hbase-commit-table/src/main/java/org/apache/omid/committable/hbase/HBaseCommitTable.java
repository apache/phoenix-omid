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
package org.apache.omid.committable.hbase;

import static org.apache.omid.committable.hbase.HBaseCommitTableConfig.COMMIT_TABLE_QUALIFIER;
import static org.apache.omid.committable.hbase.HBaseCommitTableConfig.INVALID_TX_QUALIFIER;
import static org.apache.omid.committable.hbase.HBaseCommitTableConfig.LOW_WATERMARK_QUALIFIER;
import static org.apache.omid.committable.hbase.HBaseCommitTableConfig.LOW_WATERMARK_ROW;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;


import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.CommitTable.CommitTimestamp.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Optional;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.AbstractFuture;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.SettableFuture;
import org.apache.phoenix.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.phoenix.thirdparty.com.google.protobuf.CodedOutputStream;

public class HBaseCommitTable implements CommitTable {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseCommitTable.class);

    private final Connection hbaseConnection;
    private final String tableName;
    private final byte[] commitTableFamily;
    private final byte[] lowWatermarkFamily;
    private final KeyGenerator keygen;

    /**
     * Create a hbase commit table.
     * Note that we do not take ownership of the passed htable, it is just used to construct the writer and client.
     * @throws IOException 
     */
    @Inject
    public HBaseCommitTable(Configuration hbaseConfig, HBaseCommitTableConfig config) throws IOException {
        this(ConnectionFactory.createConnection(hbaseConfig), config, KeyGeneratorImplementations.defaultKeyGenerator());
    }

    public HBaseCommitTable(Connection hbaseConnection, HBaseCommitTableConfig config) throws IOException {
        this(hbaseConnection, config, KeyGeneratorImplementations.defaultKeyGenerator());
    }

    public HBaseCommitTable(Configuration hbaseConfig, HBaseCommitTableConfig config, KeyGenerator keygen) throws IOException {
        this(ConnectionFactory.createConnection(hbaseConfig), config, keygen);
    }

    public HBaseCommitTable(Connection hbaseConnection, HBaseCommitTableConfig config, KeyGenerator keygen) throws IOException {

        this.hbaseConnection = hbaseConnection;
        this.tableName = config.getTableName();
        this.commitTableFamily = config.getCommitTableFamily();
        this.lowWatermarkFamily = config.getLowWatermarkFamily();
        this.keygen = keygen;

    }

    // ----------------------------------------------------------------------------------------------------------------
    // Reader and Writer
    // ----------------------------------------------------------------------------------------------------------------

    private class HBaseWriter implements Writer {

        private static final long INITIAL_LWM_VALUE = -1L;

        // Our own buffer for operations
        final List<Put> writeBuffer = new LinkedList<>();
        volatile long lowWatermarkToStore = INITIAL_LWM_VALUE;

        HBaseWriter() {

        }

        @Override
        public void addCommittedTransaction(long startTimestamp, long commitTimestamp) throws IOException {
            assert (startTimestamp < commitTimestamp);
            Put put = new Put(startTimestampToKey(startTimestamp), startTimestamp);
            byte[] value = encodeCommitTimestamp(startTimestamp, commitTimestamp);
            put.addColumn(commitTableFamily, COMMIT_TABLE_QUALIFIER, value);
            writeBuffer.add(put);
        }

        @Override
        public void updateLowWatermark(long lowWatermark) throws IOException {
            lowWatermarkToStore = lowWatermark;
        }

        @Override
        public void flush() throws IOException {

            try(Table table = hbaseConnection.getTable(TableName.valueOf(tableName))) {
                addLowWatermarkToStoreToWriteBuffer();
                table.put(writeBuffer);
                writeBuffer.clear();
            } catch (IOException e) {
                LOG.error("Error flushing data", e);
                throw e;
            }
        }

        @Override
        public void clearWriteBuffer() {
            writeBuffer.clear();
        }

        @Override
        public boolean atomicAddCommittedTransaction(long startTimestamp, long commitTimestamp) throws IOException {
            try (Table table = hbaseConnection.getTable(TableName.valueOf(tableName))) {
                assert (startTimestamp < commitTimestamp);
                byte[] transactionRow = startTimestampToKey(startTimestamp);
                Put put = new Put(transactionRow, startTimestamp);
                byte[] value = encodeCommitTimestamp(startTimestamp, commitTimestamp);
                put.addColumn(commitTableFamily, COMMIT_TABLE_QUALIFIER, value);
                CheckAndMutate checkAndPut = CheckAndMutate.newBuilder(transactionRow)
                        .ifNotExists(commitTableFamily, INVALID_TX_QUALIFIER)
                        .build(put);
                return table.checkAndMutate(checkAndPut).isSuccess();
            }

        }

        private void addLowWatermarkToStoreToWriteBuffer() {
            long lowWatermark = lowWatermarkToStore;
            if(lowWatermark != INITIAL_LWM_VALUE) {
                Put put = new Put(LOW_WATERMARK_ROW);
                put.addColumn(lowWatermarkFamily, LOW_WATERMARK_QUALIFIER, Bytes.toBytes(lowWatermark));
                writeBuffer.add(put);
            }
        }

    }

    class HBaseClient implements Client{

        HBaseClient(){

        }

        @Override
        public ListenableFuture<Optional<CommitTimestamp>> getCommitTimestamp(long startTimestamp) {
            startTimestamp = removeCheckpointBits(startTimestamp);
            SettableFuture<Optional<CommitTimestamp>> f = SettableFuture.create();
            try(Table table = hbaseConnection.getTable(TableName.valueOf(tableName))) {
                Get get = new Get(startTimestampToKey(startTimestamp));
                get.addColumn(commitTableFamily, COMMIT_TABLE_QUALIFIER);
                get.addColumn(commitTableFamily, INVALID_TX_QUALIFIER);

                Result result = table.get(get);

                if (containsInvalidTransaction(result)) {
                    CommitTimestamp invalidCT =
                            new CommitTimestamp(Location.COMMIT_TABLE, INVALID_TRANSACTION_MARKER, false);
                    f.set(Optional.of(invalidCT));
                    return f;
                }

                if (containsATimestamp(result)) {
                    long commitTSValue =
                            decodeCommitTimestamp(startTimestamp, result.getValue(commitTableFamily, COMMIT_TABLE_QUALIFIER));
                    CommitTimestamp validCT = new CommitTimestamp(Location.COMMIT_TABLE, commitTSValue, true);
                    f.set(Optional.of(validCT));
                } else {
                    f.set(Optional.<CommitTimestamp>absent());
                }
            } catch (IOException e) {
                LOG.error("Error getting commit timestamp for TX {}", startTimestamp, e);
                f.setException(e);
            }
            return f;
        }

        @Override
        public ListenableFuture<Long> readLowWatermark() {
            SettableFuture<Long> f = SettableFuture.create();
            try(Table table = hbaseConnection.getTable(TableName.valueOf(tableName))) {
                Get get = new Get(LOW_WATERMARK_ROW);
                get.addColumn(lowWatermarkFamily, LOW_WATERMARK_QUALIFIER);
                Result result = table.get(get);
                if (containsLowWatermark(result)) {
                    long lowWatermark = Bytes.toLong(result.getValue(lowWatermarkFamily, LOW_WATERMARK_QUALIFIER));
                    f.set(lowWatermark);
                } else {
                    f.set(0L);
                }
            } catch (IOException e) {
                LOG.error("Error getting low watermark", e);
                f.setException(e);
            }
            return f;
        }

        // This function is only used to delete a CT entry and should be renamed
        @Override
        public ListenableFuture<Void> deleteCommitEntry(long startTimestamp) {
            startTimestamp = removeCheckpointBits(startTimestamp);
            byte[] key;
            try {
                key = startTimestampToKey(startTimestamp);
            } catch (IOException e) {
                LOG.warn("Error generating timestamp for transaction completion", e);
                SettableFuture<Void> f = SettableFuture.create();
                f.setException(e);
                return f;
            }

            Delete delete = new Delete(key, startTimestamp);

            try(Table table = hbaseConnection.getTable(TableName.valueOf(tableName))) {
                table.delete(delete);
            } catch (IOException e) {
                SettableFuture<Void> f = SettableFuture.create();
                LOG.warn("Error contacting hbase", e);
                f.setException(e);
            }
            SettableFuture<Void> f = SettableFuture.create();
            f.set(null);
            return f;
        }

        @Override
        public ListenableFuture<Boolean> tryInvalidateTransaction(long startTimestamp) {
            startTimestamp = removeCheckpointBits(startTimestamp);
            SettableFuture<Boolean> f = SettableFuture.create();
            try(Table table = hbaseConnection.getTable(TableName.valueOf(tableName))) {
                byte[] row = startTimestampToKey(startTimestamp);
                Put invalidationPut = new Put(row, startTimestamp);
                invalidationPut.addColumn(commitTableFamily, INVALID_TX_QUALIFIER, Bytes.toBytes(1));

                // We need to write to the invalid column only if the commit timestamp
                // is empty. This has to be done atomically. Otherwise, if we first
                // check the commit timestamp and right before the invalidation a commit
                // timestamp is added and read by a transaction, then snapshot isolation
                // might not be hold (due to the invalidation)
                // TODO: Decide what we should we do if we can not contact the commit table. loop till succeed???
                CheckAndMutate checkAndPut = CheckAndMutate.newBuilder(row)
                        .ifNotExists(commitTableFamily, COMMIT_TABLE_QUALIFIER)
                        .build(invalidationPut);
                boolean result = table.checkAndMutate(checkAndPut).isSuccess();
                f.set(result);
            } catch (IOException ioe) {
                f.setException(ioe);
            }
            return f;
        }

        private boolean containsATimestamp(Result result) {
            return (result != null && result.containsColumn(commitTableFamily, COMMIT_TABLE_QUALIFIER));
        }

        private boolean containsInvalidTransaction(Result result) {
            return (result != null && result.containsColumn(commitTableFamily, INVALID_TX_QUALIFIER));
        }

        private boolean containsLowWatermark(Result result) {
            return (result != null && result.containsColumn(lowWatermarkFamily, LOW_WATERMARK_QUALIFIER));
        }

        private class DeleteRequest extends AbstractFuture<Void> {
            final Delete delete;

            DeleteRequest(Delete delete) {
                this.delete = delete;
            }

            void error(IOException ioe) {
                setException(ioe);
            }

            void complete() {
                set(null);
            }

            Delete getDelete() {
                return delete;
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Getters
    // ----------------------------------------------------------------------------------------------------------------

    @Override
    public Writer getWriter() throws IOException {
        return new HBaseWriter();
    }

    @Override
    public Client getClient() throws IOException {
        return new HBaseClient();
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------
    static long removeCheckpointBits(long startTimestamp) {
        return startTimestamp - (startTimestamp % CommitTable.MAX_CHECKPOINTS_PER_TXN);
    }

    private byte[] startTimestampToKey(long startTimestamp) throws IOException {
        return keygen.startTimestampToKey(startTimestamp);
    }

    private static byte[] encodeCommitTimestamp(long startTimestamp, long commitTimestamp) throws IOException {
        assert (startTimestamp < commitTimestamp);
        long diff = commitTimestamp - startTimestamp;
        byte[] bytes = new byte[CodedOutputStream.computeInt64SizeNoTag(diff)];
        CodedOutputStream cos = CodedOutputStream.newInstance(bytes);
        cos.writeInt64NoTag(diff);
        cos.flush();
        return bytes;

    }

    private static long decodeCommitTimestamp(long startTimestamp, byte[] encodedCommitTimestamp) throws IOException {
        CodedInputStream cis = CodedInputStream.newInstance(encodedCommitTimestamp);
        long diff = cis.readInt64();
        return startTimestamp + diff;
    }

}
