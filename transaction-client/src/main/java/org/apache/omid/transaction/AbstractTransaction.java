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

import org.apache.omid.tso.client.CellId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Omid's base abstract implementation of the {@link Transaction} interface.
 * Provides extra methods to access other basic transaction state required by
 * {@link TransactionManager} implementations based on snapshot
 * isolation.
 *
 * So, this abstract class must be extended by particular implementations of
 * transaction managers related to different storage systems (HBase...)
 */
public abstract class AbstractTransaction<T extends CellId> implements Transaction {

    enum VisibilityLevel {
        // Regular snapshot isolation. Returns the last key, either from the snapshot or from the current transaction
        // Sets the readTimestamp to be the writeTimestamp
        SNAPSHOT,
        // Returns all the written version of a key X that written by the transaction and the key X from the provided snapshot.
        SNAPSHOT_ALL,
        // Returns the last key, either from the snapshot or from the current transaction that was written before the last checkpoint.
        // Sets the readTimestamp to be the writeTimestamp - 1
        SNAPSHOT_EXCLUDE_CURRENT;

        public static VisibilityLevel fromInteger(int number) {
            VisibilityLevel visibilityLevel = SNAPSHOT;

            switch (number) {
            case 0:
                visibilityLevel = VisibilityLevel.SNAPSHOT;
                break;
            case 1:
                visibilityLevel =  VisibilityLevel.SNAPSHOT_ALL;
                break;
            case 2:
                visibilityLevel = VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT;
                break;
                default:
                    assert(false);
            }

            return visibilityLevel;
        }
    }

    private transient Map<String, Object> metadata = new HashMap<>();
    private final AbstractTransactionManager transactionManager;
    private final long startTimestamp;
    protected long readTimestamp;
    protected long writeTimestamp;
    private final long epoch;
    private long commitTimestamp;
    private boolean isRollbackOnly;
    private final Set<T> writeSet;
    private final Set<T> conflictFreeWriteSet;
    private Status status = Status.RUNNING;
    private VisibilityLevel visibilityLevel;

    /**
     * Base constructor
     *
     * @param transactionId
     *            transaction identifier to assign
     * @param epoch
     *            epoch of the TSOServer instance that created this transaction
     *            Used in High Availability to guarantee data consistency
     * @param writeSet
     *            initial write set for the transaction.
     *            Should be empty in most cases.
     * @param conflictFreeWriteSet
     *            initial conflict free write set for the transaction.
     *            Should be empty in most cases.
     * @param transactionManager
     *            transaction manager associated to this transaction.
     *            Usually, should be the one that created the transaction
     *            instance.
     */
    public AbstractTransaction(long transactionId,
                               long epoch,
                               Set<T> writeSet,
                               Set<T> conflictFreeWriteSet,
                               AbstractTransactionManager transactionManager) {
        this(transactionId, transactionId, VisibilityLevel.SNAPSHOT, epoch, writeSet, conflictFreeWriteSet, transactionManager);
    }

    public AbstractTransaction(long transactionId,
            long readTimestamp,
            VisibilityLevel visibilityLevel,
            long epoch,
            Set<T> writeSet,
            Set<T> conflictFreeWriteSet,
            AbstractTransactionManager transactionManager) {
        this.startTimestamp = this.writeTimestamp = transactionId;
        this.readTimestamp = readTimestamp;
        this.epoch = epoch;
        this.writeSet = writeSet;
        this.conflictFreeWriteSet = conflictFreeWriteSet;
        this.transactionManager = transactionManager;
        this.visibilityLevel = visibilityLevel;
    }

    /**
     * Creates a checkpoint and sets the visibility level to SNAPSHOT_EXCLUDE_CURRENT
     * The number of checkpoints is bounded to NUM_CHECKPOINTS in order to make checkpoint a client side operation
     * @return true if a checkpoint was created and false otherwise
     * @throws TransactionException
     */
    void checkpoint() throws TransactionException {

        setVisibilityLevel(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT);
        this.readTimestamp = this.writeTimestamp++;

        if (this.writeTimestamp % AbstractTransactionManager.MAX_CHECKPOINTS_PER_TXN == 0) {
            throw new TransactionException("Error: number of checkpoing cannot exceed " + (AbstractTransactionManager.MAX_CHECKPOINTS_PER_TXN - 1));
        }
    }

    /**
     * Allows to define specific clean-up task for transaction implementations
     */
    public abstract void cleanup();

    /**
     * @see org.apache.omid.transaction.Transaction#getTransactionId()
     */
    @Override
    public long getTransactionId() {
        return startTimestamp;
    }

    /**
     * @see org.apache.omid.transaction.Transaction#getEpoch()
     */
    @Override
    public long getEpoch() {
        return epoch;
    }

    /**
     * @see org.apache.omid.transaction.Transaction#getStatus()
     */
    @Override
    public Status getStatus() {
        return status;
    }

    /**
     * @see Transaction#isRollbackOnly()
     */
    @Override
    public void setRollbackOnly() {
        isRollbackOnly = true;
    }

    /**
     * @see org.apache.omid.transaction.Transaction#isRollbackOnly()
     */
    @Override
    public boolean isRollbackOnly() {
        return isRollbackOnly;
    }

    /**
     * Returns transaction manager associated to this transaction.
     * @return transaction manager
     */
    public AbstractTransactionManager getTransactionManager() {
        return transactionManager;
    }

    /**
     * Returns the start timestamp for this transaction.
     * @return start timestamp
     */
    public long getStartTimestamp() {
        return startTimestamp;
    }

    /**
     * Returns the read timestamp for this transaction.
     * @return read timestamp
     */
    public long getReadTimestamp() {
        return readTimestamp;
    }

    /**
     * Returns the write timestamp for this transaction.
     * @return write timestamp
     */
    public long getWriteTimestamp() {
        return writeTimestamp;
    }

    /**
     * Returns the commit timestamp for this transaction.
     * @return commit timestamp
     */
    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    /**
     * Returns the visibility level for this transaction.
     * @return visibility level
     */
    public VisibilityLevel getVisibilityLevel() {
        return visibilityLevel;
    }

    /**
     * Sets the commit timestamp for this transaction.
     * @param commitTimestamp
     *            the commit timestamp to set
     */
    public void setCommitTimestamp(long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    /**
     * Sets the visibility level for this transaction.
     * @param visibilityLevel
     *            the {@link VisibilityLevel} to set
     */
    public void setVisibilityLevel(VisibilityLevel visibilityLevel) {
        this.visibilityLevel = visibilityLevel;

        // If we are setting visibility level to either SNAPSHOT or SNAPSHOT_ALL
        // then we should let readTimestamp equals to writeTimestamp
        if (this.visibilityLevel == VisibilityLevel.SNAPSHOT ||
            this.visibilityLevel == VisibilityLevel.SNAPSHOT_ALL) {
            this.readTimestamp = this.writeTimestamp;
        }
    }

    /**
     * Sets the status for this transaction.
     * @param status
     *            the {@link Status} to set
     */
    public void setStatus(Status status) {
        this.status = status;
    }

    /**
     * Returns the current write-set for this transaction.
     * @return write set
     */
    public Set<T> getWriteSet() {
        return writeSet;
    }

    /**
     * Returns the current write-set for this transaction that its elements are not candidates for conflict analysis.
     * @return conflictFreeWriteSet
     */
    public Set<T> getConflictFreeWriteSet() {
        return conflictFreeWriteSet;
    }

    /**
     * Adds an element to the transaction write-set.
     * @param element
     *            the element to add
     */
    public void addWriteSetElement(T element) {
        writeSet.add(element);
    }

    /**
     * Adds an element to the transaction conflict free write-set.
     * @param element
     *            the element to add
     */
    public void addConflictFreeWriteSetElement(T element) {
        conflictFreeWriteSet.add(element);
    }

    @Override
    public String toString() {
        return String.format("Tx-%s [%s] (ST=%d, RT=%d, WT=%d, CT=%d, Epoch=%d) WriteSet %s ConflictFreeWriteSet %s",
                             Long.toHexString(getTransactionId()),
                             status,
                             startTimestamp,
                             readTimestamp,
                             writeTimestamp,
                             commitTimestamp,
                             epoch,
                             writeSet,
                             conflictFreeWriteSet);
    }

    @Override
    public Optional<Object> getMetadata(String key) {
        return Optional.fromNullable(metadata.get(key));
    }

    /**
     * Expects they metadata stored under key "key" to be of the "Set" type,
     * append "value" to the existing set or creates a new one
     */
    @Override
    @SuppressWarnings("unchecked")
    public void appendMetadata(String key, Object value) {
        List existingValue = (List) metadata.get(key);
        if (existingValue == null) {
            List<Object> newList = new ArrayList<>();
            newList.add(value);
            metadata.put(key, newList);
        } else {
            existingValue.add(value);
        }
    }

    @Override
    public void setMetadata(String key, Object value) {
        metadata.put(key, value);
    }

}
