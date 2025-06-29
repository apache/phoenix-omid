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

import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.NetworkUtils;
import org.apache.omid.TestUtils;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.InMemoryCommitTable;
import org.apache.omid.committable.hbase.HBaseCommitTableConfig;
import org.apache.omid.timestamp.storage.HBaseTimestampStorageConfig;
import org.apache.omid.tools.hbase.OmidTableManager;
import org.apache.omid.tso.TSOMockModule;
import org.apache.omid.tso.TSOServer;
import org.apache.omid.tso.TSOServerConfig;
import org.apache.omid.tso.TSOServerConfig.TIMESTAMP_TYPE;
import org.apache.omid.tso.client.OmidClientConfiguration;
import org.apache.omid.tso.client.TSOClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.AfterGroups;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.BeforeMethod;

import com.google.inject.Guice;
import com.google.inject.Injector;

public abstract class OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OmidTestBase.class);

    static HBaseTestingUtility hBaseUtils;
    private static MiniHBaseCluster hbaseCluster;
    static Configuration hbaseConf;
    static Connection connection;

    protected static final String TEST_TABLE = "test";
    protected static final String TEST_FAMILY = "data";
    static final String TEST_FAMILY2 = "data2";
    public static int port;

    private HBaseCommitTableConfig hBaseCommitTableConfig;

    @BeforeMethod(alwaysRun = true)
    public void beforeClass(Method method) throws Exception {
        Thread.currentThread().setName("UnitTest-" + method.getName());
    }


    @BeforeGroups(groups = "sharedHBase")
    public void beforeGroups(ITestContext context) throws Exception {
        // TSO Setup
        TSOServerConfig tsoConfig = new TSOServerConfig();
        port = NetworkUtils.getFreePort();
        tsoConfig.setPort(port);
        tsoConfig.setConflictMapSize(1000);
        tsoConfig.setWaitStrategy("LOW_CPU");
        tsoConfig.setTimestampType(TIMESTAMP_TYPE.INCREMENTAL.toString());
        Injector injector = Guice.createInjector(new TSOMockModule(tsoConfig));
        LOG.info("Starting TSO");
        TSOServer tso = injector.getInstance(TSOServer.class);
        hBaseCommitTableConfig = injector.getInstance(HBaseCommitTableConfig.class);
        HBaseTimestampStorageConfig hBaseTimestampStorageConfig = injector.getInstance(HBaseTimestampStorageConfig.class);
        tso.startAsync();
        tso.awaitRunning();
        TestUtils.waitForSocketListening("localhost", port, 100);
        LOG.info("Finished loading TSO");
        context.setAttribute("tso", tso);

        OmidClientConfiguration clientConf = new OmidClientConfiguration();
        clientConf.setConnectionString("localhost:" + port);
        context.setAttribute("clientConf", clientConf);

        InMemoryCommitTable commitTable = (InMemoryCommitTable) injector.getInstance(CommitTable.class);
        context.setAttribute("commitTable", commitTable);

        // Create the associated Handler
        TSOClient client = TSOClient.newInstance(clientConf);
        context.setAttribute("client", client);

        // ------------------------------------------------------------------------------------------------------------
        // HBase setup
        // ------------------------------------------------------------------------------------------------------------
        LOG.info("Creating HBase minicluster");
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.setInt("hbase.hregion.memstore.flush.size", 10_000 * 1024);
        hbaseConf.setInt("hbase.regionserver.nbreservationblocks", 1);
        hbaseConf.setInt(HBASE_CLIENT_RETRIES_NUMBER, 3);

        File tempFile = Files.createTempFile("OmidTest", "").toFile();
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

    protected void createTestTable() throws IOException {
        Admin admin = hBaseUtils.getAdmin();
        ArrayList<ColumnFamilyDescriptor> fams = new ArrayList<>();
        fams.add(ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes(TEST_FAMILY))
                .setMaxVersions(Integer.MAX_VALUE)
                .build());
        fams.add(ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes(TEST_FAMILY2))
                .setMaxVersions(Integer.MAX_VALUE)
                .build());
        TableDescriptor test_table_desc = TableDescriptorBuilder
                .newBuilder(TableName.valueOf(TEST_TABLE))
                .setColumnFamilies(fams)
                .build();

        admin.createTable(test_table_desc);
    }

    private void createCommitTable() throws IOException {
        String[] args = new String[]{OmidTableManager.COMMIT_TABLE_COMMAND_NAME, "-numRegions", "1"};
        OmidTableManager omidTableManager = new OmidTableManager(args);
        omidTableManager.executeActionsOnHBase(hbaseConf);
    }


    private TSOServer getTSO(ITestContext context) {
        return (TSOServer) context.getAttribute("tso");
    }


    TSOClient getClient(ITestContext context) {
        return (TSOClient) context.getAttribute("client");
    }

    InMemoryCommitTable getCommitTable(ITestContext context) {
        return (InMemoryCommitTable) context.getAttribute("commitTable");
    }

    protected TransactionManager newTransactionManager(ITestContext context) throws Exception {
        return newTransactionManager(context, getClient(context));
    }

    protected TransactionManager newTransactionManager(ITestContext context, PostCommitActions postCommitActions) throws Exception {
        HBaseOmidClientConfiguration clientConf = new HBaseOmidClientConfiguration();
        clientConf.setConnectionString("localhost:" + port);
        clientConf.setHBaseConfiguration(hbaseConf);
        return HBaseTransactionManager.builder(clientConf)
                .postCommitter(postCommitActions)
                .commitTableClient(getCommitTable(context).getClient())
                .commitTableWriter(getCommitTable(context).getWriter())
                .tsoClient(getClient(context)).build();
    }

    protected TransactionManager newTransactionManager(ITestContext context, TSOClient tsoClient) throws Exception {
        HBaseOmidClientConfiguration clientConf = new HBaseOmidClientConfiguration();
        clientConf.setConnectionString("localhost:" + port);
        clientConf.setHBaseConfiguration(hbaseConf);
        return HBaseTransactionManager.builder(clientConf)
                .commitTableClient(getCommitTable(context).getClient())
                .commitTableWriter(getCommitTable(context).getWriter())
                .tsoClient(tsoClient).build();
    }

    protected TransactionManager newTransactionManager(ITestContext context, CommitTable.Client commitTableClient)
            throws Exception {
        HBaseOmidClientConfiguration clientConf = new HBaseOmidClientConfiguration();
        clientConf.setConnectionString("localhost:" + port);
        clientConf.setHBaseConfiguration(hbaseConf);
        return HBaseTransactionManager.builder(clientConf)
                .commitTableClient(commitTableClient)
                .commitTableWriter(getCommitTable(context).getWriter())
                .tsoClient(getClient(context)).build();
    }

    @AfterGroups(groups = "sharedHBase")
    public void afterGroups(ITestContext context) throws Exception {
        LOG.info("Tearing down OmidTestBase...");
        if (hbaseCluster != null) {
            hBaseUtils.shutdownMiniCluster();
        }

        getClient(context).close().get();
        getTSO(context).stopAsync();
        getTSO(context).awaitTerminated();
        TestUtils.waitForSocketNotListening("localhost", port, 1000);
    }

    @AfterMethod(groups = "sharedHBase", timeOut = 60_000)
    public void afterMethod() {
        try {
            LOG.info("tearing Down");
            Admin admin = hBaseUtils.getAdmin();
            deleteTable(admin, TableName.valueOf(TEST_TABLE));
            createTestTable();
            if (hBaseCommitTableConfig != null) {
                deleteTable(admin, TableName.valueOf(hBaseCommitTableConfig.getTableName()));
            }
            createCommitTable();
        } catch (Exception e) {
            LOG.error("Error tearing down", e);
        }
    }

    void deleteTable(Admin admin, TableName tableName) throws IOException {
        if (admin.tableExists(tableName)) {
            if (admin.isTableDisabled(tableName)) {
                admin.deleteTable(tableName);
            } else {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
        }
    }

    static boolean verifyValue(Table table, byte[] row,
                               byte[] fam, byte[] col, byte[] value) {

        try {
            Get g = new Get(row).readVersions(1);
            Result r = table.get(g);
            Cell cell = r.getColumnLatestCell(fam, col);

            if (LOG.isTraceEnabled()) {
                LOG.trace("Value for " + table.getName().getNameAsString() + ":"
                                  + Bytes.toString(row) + ":" + Bytes.toString(fam)
                                  + Bytes.toString(col) + "=>" + Bytes.toString(CellUtil.cloneValue(cell))
                                  + " (" + Bytes.toString(value) + " expected)");
            }

            return Bytes.equals(CellUtil.cloneValue(cell), value);
        } catch (IOException e) {
            LOG.error("Error reading row " + table.getName().getNameAsString() + ":"
                              + Bytes.toString(row) + ":" + Bytes.toString(fam)
                              + Bytes.toString(col), e);
            return false;
        }
    }
}
