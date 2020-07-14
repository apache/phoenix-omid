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
package org.apache.omid.tso;


import org.apache.phoenix.thirdparty.com.google.common.base.Optional;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.omid.TestUtils;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.proto.TSOProto;
import org.apache.omid.tso.client.CellId;
import org.apache.omid.tso.client.TSOClient;
import org.apache.omid.tso.client.TSOClientOneShot;
import org.apache.omid.tso.util.DummyCellIdImpl;
import org.testng.annotations.Test;
import org.apache.omid.tso.client.OmidClientConfiguration;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import java.util.Set;
import static org.testng.Assert.assertTrue;

public class TestTSOLL {

    private static final Logger LOG = LoggerFactory.getLogger(TestTSOLL.class);

    private static final String TSO_SERVER_HOST = "localhost";
    private static final int TSO_SERVER_PORT = 1234;


    private OmidClientConfiguration tsoClientConf;

    // Required infrastructure for TSOClient test
    private TSOServer tsoServer;
    private PausableTimestampOracle pausableTSOracle;
    private CommitTable commitTable;

    private final static CellId c1 = new DummyCellIdImpl(0xdeadbeefL);
    private final static CellId c2 = new DummyCellIdImpl(0xfeedcafeL);

    private final static Set<CellId> testWriteSet = Sets.newHashSet(c1, c2);

    @Mock
    ReplyProcessor replyProcessor;

    @BeforeMethod
    public void beforeMethod() throws Exception {

        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setLowLatency(true);
        tsoConfig.setConflictMapSize(1000);
        tsoConfig.setPort(TSO_SERVER_PORT);
        tsoConfig.setNumConcurrentCTWriters(2);
        Module tsoServerMockModule = new TSOMockModule(tsoConfig);
        Injector injector = Guice.createInjector(tsoServerMockModule);

        LOG.info("==================================================================================================");
        LOG.info("======================================= Init TSO Server ==========================================");
        LOG.info("==================================================================================================");

        tsoServer = injector.getInstance(TSOServer.class);
        tsoServer.startAsync();
        tsoServer.awaitRunning();
        TestUtils.waitForSocketListening(TSO_SERVER_HOST, TSO_SERVER_PORT, 100);

        LOG.info("==================================================================================================");
        LOG.info("===================================== TSO Server Initialized =====================================");
        LOG.info("==================================================================================================");

        pausableTSOracle = (PausableTimestampOracle) injector.getInstance(TimestampOracle.class);
        commitTable = injector.getInstance(CommitTable.class);

        OmidClientConfiguration tsoClientConf = new OmidClientConfiguration();
        tsoClientConf.setConnectionString(TSO_SERVER_HOST + ":" + TSO_SERVER_PORT);

        this.tsoClientConf = tsoClientConf;
        commitTable = injector.getInstance(CommitTable.class);
        replyProcessor = injector.getInstance(ReplyProcessor.class);
    }

    @AfterMethod
    public void afterMethod() throws Exception {


        tsoServer.stopAsync();
        tsoServer.awaitTerminated();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_SERVER_HOST, TSO_SERVER_PORT, 1000);

        pausableTSOracle.resume();

    }

    @Test(timeOut = 30_000)
    public void testNoWriteToCommitTable() throws Exception {

        TSOClient client = TSOClient.newInstance(tsoClientConf);
        TSOClientOneShot clientOneShot = new TSOClientOneShot(TSO_SERVER_HOST, TSO_SERVER_PORT);
        long ts1 = client.getNewStartTimestamp().get();

        TSOProto.Response response1 = clientOneShot.makeRequest(createCommitRequest(ts1, false, testWriteSet));
        assertTrue(response1.getCommitResponse().hasCommitTimestamp());
        Optional<CommitTable.CommitTimestamp> cts = commitTable.getClient().getCommitTimestamp(ts1).get();

        assertTrue(cts.isPresent() == false);
    }

    private TSOProto.Request createCommitRequest(long ts, boolean retry, Set<CellId> writeSet) {
        TSOProto.Request.Builder builder = TSOProto.Request.newBuilder();
        TSOProto.CommitRequest.Builder commitBuilder = TSOProto.CommitRequest.newBuilder();
        commitBuilder.setStartTimestamp(ts);
        commitBuilder.setIsRetry(retry);
        for (CellId cell : writeSet) {
            commitBuilder.addCellId(cell.getCellId());
        }
        return builder.setCommitRequest(commitBuilder.build()).build();
    }
}