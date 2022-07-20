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
package org.apache.omid.timestamp.storage;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores the max timestamp assigned by the TO in HBase.
 * It's always written non-transactionally in the same row and column
 */
public class HBaseTimestampStorage implements TimestampStorage {

    private static final long INITIAL_MAX_TS_VALUE = 0;

    private static final Logger LOG = LoggerFactory.getLogger(HBaseTimestampStorage.class);

    // ROW and COLUMN to write the assigned timestamp
    private static final byte[] TSO_ROW = "MAX_TIMESTAMP_R".getBytes(UTF_8);
    private static final byte[] TSO_QUALIFIER = "MAX_TIMESTAMP_Q".getBytes(UTF_8);

    private final Table table;
    private final byte[] cfName;
    private final Connection connection;

    @Inject
    public HBaseTimestampStorage(Configuration hbaseConfig, HBaseTimestampStorageConfig config) throws IOException {
        connection = ConnectionFactory.createConnection(hbaseConfig);
        this.table = connection.getTable(TableName.valueOf(config.getTableName()));
        this.cfName = config.getFamilyName().getBytes(UTF_8);
    }

    @Override
    public void updateMaxTimestamp(long previousMaxTimestamp, long newMaxTimestamp) throws IOException {
        if (newMaxTimestamp < 0) {
            LOG.error("Negative value received for maxTimestamp: {}", newMaxTimestamp);
            throw new IllegalArgumentException("Negative value received for maxTimestamp" + newMaxTimestamp);
        }
        Put put = new Put(TSO_ROW);
        put.addColumn(cfName, TSO_QUALIFIER, Bytes.toBytes(newMaxTimestamp));
        byte[] previousVal = null;
        if (previousMaxTimestamp != INITIAL_MAX_TS_VALUE) {
            previousVal = Bytes.toBytes(previousMaxTimestamp);
        }
        CheckAndMutate checkAndPut = CheckAndMutate.newBuilder(TSO_ROW)
                .ifEquals(cfName, TSO_QUALIFIER, previousVal)
                .build(put);
        if (!table.checkAndMutate(checkAndPut).isSuccess()) {
            throw new IOException("Previous max timestamp is incorrect");
        }
    }

    @Override
    public long getMaxTimestamp() throws IOException {
        Get get = new Get(TSO_ROW);
        get.addColumn(cfName, TSO_QUALIFIER);

        Result result = table.get(get);
        if (result.containsColumn(cfName, TSO_QUALIFIER)) {
            return Bytes.toLong(result.getValue(cfName, TSO_QUALIFIER));
        } else {
            // This happens for example when a new cluster is created
            return INITIAL_MAX_TS_VALUE;
        }

    }


    public void close() throws IOException {
        //TODO this is never called
        table.close();
        connection.close();
    }
}
