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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.ipc.controller.InterRegionServerRpcControllerFactory;
import org.apache.omid.HBaseShims;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionConnectionFactory {
    public static final String COMMIT_TABLE_ACCESS_ON_COMPACTION_RETRIES_NUMBER = "omid.commit.table.access.on.compaction.retries";
    public static final String COMMIT_TABLE_ACCESS_ON_COMPACTION_RETRY_PAUSE = "omid.commit.table.access.on.compaction.retry.pause";
    public static final String COMMIT_TABLE_ACCESS_ON_READ_RETRIES_NUMBER = "omid.commit.table.access.on.read.retries";
    public static final String COMMIT_TABLE_ACCESS_ON_READ_RETRY_PAUSE = "omid.commit.table.access.on.read.retry.pause";

    private static final Logger LOG = LoggerFactory.getLogger(RegionConnectionFactory.class);
    private static final int DEFAULT_COMMIT_TABLE_ACCESS_ON_COMPACTION_RETRIES_NUMBER = 20;
    private static final int DEFAULT_COMMIT_TABLE_ACCESS_ON_COMPACTION_RETRY_PAUSE = 100;
    // This setting controls how many retries occur on the region server if an
    // IOException occurs while trying to access the commit table. Because a
    // handler thread will be in use while these retries occur and the client
    // will be blocked waiting, it must not tie up the call for longer than
    // the client RPC timeout. Otherwise, the client will initiate retries on it's
    // end, tying up yet another handler thread. It's best if the retries can be
    // zero, as in that case the handler is released and the retries occur on the
    // client side. In testing, we've seen NoServerForRegionException occur which
    // is a DoNotRetryIOException which are not retried on the client. It's not
    // clear if this is a real issue or a test-only issue.
    private static final int DEFAULT_COMMIT_TABLE_ACCESS_ON_READ_RETRIES_NUMBER = HBaseShims.getNoRetriesNumber() + 1;
    private static final int DEFAULT_COMMIT_TABLE_ACCESS_ON_READ_RETRY_PAUSE = 100;

    private RegionConnectionFactory() {
    }
    
    public static enum ConnectionType {
        COMPACTION_CONNECTION,
        READ_CONNECTION,
        DEFAULT_SERVER_CONNECTION;
    }

    private static Map<ConnectionType, Connection> connections =
            new HashMap<ConnectionType, Connection>();

    /**
     * Utility to work around the limitation of the copy constructor
     * {@link Configuration#Configuration(Configuration)} provided by the {@link Configuration}
     * class. See https://issues.apache.org/jira/browse/HBASE-18378.
     * The copy constructor doesn't copy all the config settings, so we need to resort to
     * iterating through all the settings and setting it on the cloned config.
     * @param toCopy  configuration to copy
     * @return
     */
    private static Configuration cloneConfig(Configuration toCopy) {
        Configuration clone = new Configuration();
        Iterator<Entry<String, String>> iterator = toCopy.iterator();
        while (iterator.hasNext()) {
            Entry<String, String> entry = iterator.next();
            clone.set(entry.getKey(), entry.getValue());
        }
        return clone;
    }
    
    public static void shutdown() {
        synchronized (RegionConnectionFactory.class) {
            for (Connection connection : connections.values()) {
                try {
                    connection.close();
                } catch (IOException e) {
                    LOG.warn("Unable to close coprocessor connection", e);
                }
            }
            connections.clear();
        }
    }


    public static Connection getConnection(final ConnectionType connectionType, final RegionCoprocessorEnvironment env) throws IOException {
        Connection connection = null;
        if((connection = connections.get(connectionType)) == null) {
            synchronized (RegionConnectionFactory.class) {
                if((connection = connections.get(connectionType)) == null) {
                    connection = HBaseShims.newServerConnection(getTypeSpecificConfiguration(connectionType, env.getConfiguration()), env);
                    connections.put(connectionType, connection);
                    return connection;
                }
            }
        }
        return connection;
    }

    private static Configuration getTypeSpecificConfiguration(ConnectionType connectionType, Configuration conf) {
        switch (connectionType) {
        case COMPACTION_CONNECTION:
            return getCompactionConfig(conf);
        case DEFAULT_SERVER_CONNECTION:
            return conf;
        case READ_CONNECTION:
            return getReadConfig(conf);
        default:
            return conf;
        }
    }
    
    private static Configuration getCompactionConfig(Configuration conf) {
        Configuration compactionConfig = cloneConfig(conf);
        // lower the number of rpc retries, so we don't hang the compaction
        compactionConfig.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
                conf.getInt(COMMIT_TABLE_ACCESS_ON_COMPACTION_RETRIES_NUMBER,
                        DEFAULT_COMMIT_TABLE_ACCESS_ON_COMPACTION_RETRIES_NUMBER));
        compactionConfig.setInt(HConstants.HBASE_CLIENT_PAUSE,
                conf.getInt(COMMIT_TABLE_ACCESS_ON_COMPACTION_RETRY_PAUSE,
                        DEFAULT_COMMIT_TABLE_ACCESS_ON_COMPACTION_RETRY_PAUSE));
        return compactionConfig;
    }

    private static Configuration getReadConfig(Configuration conf) {
        Configuration compactionConfig = cloneConfig(conf);
        // lower the number of rpc retries, so we don't hang the compaction
        compactionConfig.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
                conf.getInt(COMMIT_TABLE_ACCESS_ON_READ_RETRIES_NUMBER,
                        DEFAULT_COMMIT_TABLE_ACCESS_ON_READ_RETRIES_NUMBER));
        compactionConfig.setInt(HConstants.HBASE_CLIENT_PAUSE,
                conf.getInt(COMMIT_TABLE_ACCESS_ON_READ_RETRY_PAUSE,
                        DEFAULT_COMMIT_TABLE_ACCESS_ON_READ_RETRY_PAUSE));
        return compactionConfig;
    }
}
