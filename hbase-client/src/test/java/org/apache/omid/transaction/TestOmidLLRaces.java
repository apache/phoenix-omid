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
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;
import static org.apache.omid.committable.hbase.HBaseCommitTableConfig.DEFAULT_COMMIT_TABLE_CF_NAME;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.omid.committable.hbase.KeyGenerator;
import org.apache.omid.committable.hbase.KeyGeneratorImplementations;

import org.apache.omid.tso.client.OmidClientConfiguration;
import org.apache.omid.tso.client.TSOClient;

import org.testng.ITestContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import org.apache.hadoop.hbase.client.HBaseAdmin;

import org.apache.omid.TestUtils;


import org.apache.omid.timestamp.storage.HBaseTimestampStorageConfig;
import org.apache.omid.tools.hbase.OmidTableManager;
import org.apache.omid.tso.TSOMockModule;
import org.apache.omid.tso.TSOServer;
import org.apache.omid.tso.TSOServerConfig;


public class TestOmidLLRaces {

    static HBaseTestingUtility hBaseUtils;
    private static MiniHBaseCluster hbaseCluster;
    static Configuration hbaseConf;
    static Connection connection;

    private static final String TEST_FAMILY = "data";
    static final String TEST_FAMILY2 = "data2";
    private static final String TEST_TABLE = "test";
    private static final byte[] row1 = Bytes.toBytes("test-is-committed1");
    private static final byte[] row2 = Bytes.toBytes("test-is-committed2");
    private static final byte[] family = Bytes.toBytes("data");
    private static final byte[] qualifier = Bytes.toBytes("testdata");
    private static final byte[] data1 = Bytes.toBytes("testWrite-1");

    private static final Logger LOG = LoggerFactory.getLogger(TestOmidLLRaces.class);
    private TSOClient client;

    @BeforeClass
    public void setup() throws Exception {
        // TSO Setup
        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setPort(1234);
        tsoConfig.setConflictMapSize(1000);
        tsoConfig.setLowLatency(true);
        tsoConfig.setWaitStrategy("LOW_CPU");
        Injector injector = Guice.createInjector(new TSOMockModule(tsoConfig));
        LOG.info("Starting TSO");
        TSOServer tso = injector.getInstance(TSOServer.class);
        HBaseTimestampStorageConfig hBaseTimestampStorageConfig = injector.getInstance(HBaseTimestampStorageConfig.class);
        tso.startAndWait();
        TestUtils.waitForSocketListening("localhost", 1234, 100);
        LOG.info("Finished loading TSO");

        OmidClientConfiguration clientConf = new OmidClientConfiguration();
        clientConf.setConnectionString("localhost:1234");

        // Create the associated Handler
        client = TSOClient.newInstance(clientConf);

        // ------------------------------------------------------------------------------------------------------------
        // HBase setup
        // ------------------------------------------------------------------------------------------------------------
        LOG.info("Creating HBase minicluster");
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.setInt("hbase.hregion.memstore.flush.size", 10_000 * 1024);
        hbaseConf.setInt("hbase.regionserver.nbreservationblocks", 1);
        hbaseConf.setInt(HBASE_CLIENT_RETRIES_NUMBER, 3);

        File tempFile = File.createTempFile("OmidTest", "");
        tempFile.deleteOnExit();
        hbaseConf.set("hbase.rootdir", tempFile.getAbsolutePath());
        hbaseConf.setBoolean("hbase.localcluster.assign.random.ports",true);
        hBaseUtils = new HBaseTestingUtility(hbaseConf);
        hbaseCluster = hBaseUtils.startMiniCluster(1);
        connection = ConnectionFactory.createConnection(hbaseConf);
        hBaseUtils.createTable(TableName.valueOf(hBaseTimestampStorageConfig.getTableName()),
                new byte[][]{hBaseTimestampStorageConfig.getFamilyName().getBytes()},
                Integer.MAX_VALUE);
        createTestTable();
        createCommitTable();

        LOG.info("HBase minicluster is up");
    }


    private void createCommitTable() throws IOException {
        String[] args = new String[]{OmidTableManager.COMMIT_TABLE_COMMAND_NAME, "-numRegions", "1"};
        OmidTableManager omidTableManager = new OmidTableManager(args);
        omidTableManager.executeActionsOnHBase(hbaseConf);
    }

    private void createTestTable() throws IOException {
        HBaseAdmin admin = hBaseUtils.getHBaseAdmin();
        HTableDescriptor test_table_desc = new HTableDescriptor(TableName.valueOf(TEST_TABLE));
        HColumnDescriptor datafam = new HColumnDescriptor(TEST_FAMILY);
        HColumnDescriptor datafam2 = new HColumnDescriptor(TEST_FAMILY2);
        datafam.setMaxVersions(Integer.MAX_VALUE);
        datafam2.setMaxVersions(Integer.MAX_VALUE);
        test_table_desc.addFamily(datafam);
        test_table_desc.addFamily(datafam2);
        admin.createTable(test_table_desc);
    }

    protected TransactionManager newTransactionManagerHBaseCommitTable(TSOClient tsoClient) throws Exception {
        HBaseOmidClientConfiguration clientConf = new HBaseOmidClientConfiguration();
        clientConf.setConnectionString("localhost:1234");
        clientConf.setHBaseConfiguration(hbaseConf);
        return HBaseTransactionManager.builder(clientConf)
                .tsoClient(tsoClient).build();
    }


    @Test(timeOut = 30_000)
    public void testIsCommitted() throws Exception {
        AbstractTransactionManager tm = (AbstractTransactionManager)newTransactionManagerHBaseCommitTable(client);

        Table htable = connection.getTable(TableName.valueOf(TEST_TABLE));
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(new HTableAccessWrapper(htable, htable),
                tm.getCommitTableClient());
        TTable table = spy(new TTable(htable, snapshotFilter, false));

        HBaseTransaction t1 = (HBaseTransaction) tm.begin();

        Put put = new Put(row1);
        put.addColumn(family, qualifier, data1);
        table.put(t1, put);
        tm.commit(t1);

        HBaseTransaction t2 = (HBaseTransaction) tm.begin();
        put = new Put(row2);
        put.addColumn(family, qualifier, data1);
        table.put(t2, put);
        table.flushCommits();

        HBaseTransaction t3 = (HBaseTransaction) tm.begin();
        put = new Put(row2);
        put.addColumn(family, qualifier, data1);
        table.put(t3, put);
        tm.commit(t3);

        HBaseCellId hBaseCellId1 = HBaseCellId.valueOf(t1, table, row1, family, qualifier, t1.getStartTimestamp());
        HBaseCellId hBaseCellId2 = HBaseCellId.valueOf(t2, table, row2, family, qualifier, t2.getStartTimestamp());
        HBaseCellId hBaseCellId3 = HBaseCellId.valueOf(t3, table, row2, family, qualifier, t3.getStartTimestamp());

        assertTrue(snapshotFilter.isCommitted(hBaseCellId1, 0, false), "row1 should be committed");
        assertFalse(snapshotFilter.isCommitted(hBaseCellId2, 0, false), "row2 should not be committed for kv2");
        assertTrue(snapshotFilter.isCommitted(hBaseCellId3, 0, false), "row2 should be committed for kv3");
        assertTrue(tm.isLowLatency());
    }


    @Test(timeOut = 30_000)
    public void testInvalidation(ITestContext context) throws Exception {
        AbstractTransactionManager tm = (AbstractTransactionManager)newTransactionManagerHBaseCommitTable(client);

        Table htable = connection.getTable(TableName.valueOf(TEST_TABLE));
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(new HTableAccessWrapper(htable, htable),
                tm.getCommitTableClient());
        TTable table = spy(new TTable(htable, snapshotFilter, false));

        HBaseTransaction t1 = (HBaseTransaction) tm.begin();
        Put put = new Put(row1);
        put.addColumn(family, qualifier, data1);
        table.put(t1, put);

        HBaseTransaction t2 = (HBaseTransaction) tm.begin();
        Get get = new Get(row1);
        get.addColumn(family, qualifier);
        table.get(t2,get);

        //assert there is an invalidation marker:
        Table commitTable = connection.getTable(TableName.valueOf("OMID_COMMIT_TABLE"));
        KeyGenerator keygen = KeyGeneratorImplementations.defaultKeyGenerator();
        byte[] row = keygen.startTimestampToKey(t1.getStartTimestamp());
        Get getInvalidation = new Get(row);
        getInvalidation.addColumn(Bytes.toBytes(DEFAULT_COMMIT_TABLE_CF_NAME),"IT".getBytes(UTF_8));
        Result res = commitTable.get(getInvalidation);
        int val = Bytes.toInt(res.getValue(Bytes.toBytes(DEFAULT_COMMIT_TABLE_CF_NAME), "IT".getBytes(UTF_8)));
        assertTrue(val == 1);

        boolean gotInvalidated = false;
        try {
            tm.commit(t1);
        } catch (RollbackException e) {
            gotInvalidated = true;
        }
        assertTrue(gotInvalidated);
        tm.commit(t2);
        Thread.sleep(1000);
        res = commitTable.get(getInvalidation);
        assertTrue(res.isEmpty());
        assertTrue(tm.isLowLatency());
    }
}
