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

import static org.apache.phoenix.thirdparty.com.google.common.base.Charsets.UTF_8;

import org.apache.omid.tso.client.CellId;

import org.apache.phoenix.thirdparty.com.google.common.hash.Hasher;
import org.apache.phoenix.thirdparty.com.google.common.hash.Hashing;

public class HBaseCellId implements CellId {

    private final TTable table;
    private final byte[] row;
    private final byte[] family;
    private final byte[] qualifier;
    private long timestamp;

    public HBaseCellId(TTable table, byte[] row, byte[] family, byte[] qualifier, long timestamp) {
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
        return new String(table.getTableName(), UTF_8)
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
}
