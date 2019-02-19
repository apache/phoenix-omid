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

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.omid.transaction.CellUtils.SHARED_FAMILY_QUALIFIER;

import com.google.common.base.Charsets;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.tso.client.CellId;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.util.Arrays;
import java.util.Objects;

public class HBaseCellId implements CellId {



    private final TTable table;
    private final byte[] row;
    private final byte[] family;
    private final byte[] qualifier;
    private long timestamp;

    private HBaseCellId(TTable table, byte[] row, byte[] family, byte[] qualifier, long timestamp) {
        this.timestamp = timestamp;
        this.table = table;
        this.row = row;
        this.family = family;
        this.qualifier = qualifier;
    }

    public TTable getTable() {
        return table;
    }

    public byte[] getRow() {
        return row;
    }

    public byte[] getFamily() {
        return family;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return (table == null?"NO_TABLE":new String(table.getTableName(), UTF_8))
                + ":" + new String(row, UTF_8)
                + ":" + new String(family, UTF_8)
                + ":" + new String(qualifier, UTF_8)
                + ":" + timestamp;
    }

    @Override
    public long getCellId() {
        return getHasher()
                .putBytes(table.getTableName())
                .putBytes(row)
                .putBytes(family)
                .putBytes(qualifier)
                .hash().asLong();
    }

    @Override
    public long getTableId() {
        return getHasher()
                .putBytes(table.getTableName())
                .hash().asLong();
    }

    @Override
    public long getRowId() {
        return getHasher()
                .putBytes(table.getTableName())
                .putBytes(row)
                .hash().asLong();
    }

    public static Hasher getHasher() {
        return Hashing.murmur3_128().newHasher();
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(table.getTableName()),
                Arrays.hashCode(row),
                Arrays.hashCode(family),
                Arrays.hashCode(qualifier));
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        assert(o instanceof HBaseCellId);
        HBaseCellId other = (HBaseCellId) o;

        return Arrays.equals(table.getTableName(), other.getTable().getTableName()) &&
                Arrays.equals(row, other.getRow()) &&
                Arrays.equals(family, other.getFamily()) &&
                Arrays.equals(qualifier, other.getQualifier());
    }


    public int compareTo(CellId o) {
        assert(o instanceof HBaseCellId);
        HBaseCellId other = (HBaseCellId) o;

        int tableCompare = Bytes.compareTo(table.getTableName(), other.getTable().getTableName());
        if (tableCompare != 0) {
            return tableCompare;
        } else {
            int rowCompare = Bytes.compareTo(row, other.getRow());
            if (rowCompare != 0) {
                return rowCompare;
            } else {
                int familyCompare = Bytes.compareTo(family, other.getFamily());
                if (familyCompare != 0) {
                    return familyCompare;
                } else {
                    return Bytes.compareTo(qualifier, other.getQualifier());
                }
            }
        }
    }
    public static HBaseCellId valueOf(Transaction tx, TTable table, byte[] row, byte[] family, byte[] qualifier, long timestamp) {
        if (((HBaseTransaction)tx).getConflictDetectionLevel() == HBaseTransactionManager.ConflictDetectionLevel.ROW) {
            return new HBaseCellId(table, row, family, SHARED_FAMILY_QUALIFIER, timestamp);
        } else {
            return new HBaseCellId(table, row, family, qualifier, timestamp);
        }
    }

    public static HBaseCellId valueOf(HBaseTransactionManager.ConflictDetectionLevel conflictDetectionLevel,
                                      TTable table, byte[] row, byte[] family, byte[] qualifier, long timestamp) {
        if (conflictDetectionLevel == HBaseTransactionManager.ConflictDetectionLevel.ROW) {
            return new HBaseCellId(table, row, family, SHARED_FAMILY_QUALIFIER, timestamp);
        } else {
            return new HBaseCellId(table, row, family, qualifier, timestamp);
        }
    }



}
