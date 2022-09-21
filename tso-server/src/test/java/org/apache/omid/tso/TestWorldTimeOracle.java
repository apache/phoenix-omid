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

import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.timestamp.storage.TimestampStorage;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertTrue;

public class TestWorldTimeOracle {

    private static final Logger LOG = LoggerFactory.getLogger(TestWorldTimeOracle.class);

    @Mock
    private MetricsRegistry metrics;
    @Mock
    private Panicker panicker;
    @Mock
    private TimestampStorage timestampStorage;
    @Mock
    private TSOServerConfig config;

    // Component under test
    @InjectMocks
    private WorldClockOracleImpl worldClockOracle;

    @BeforeMethod(alwaysRun = true, timeOut = 30_000)
    public void initMocksAndComponents() {
        MockitoAnnotations.initMocks(this);
    }

    @Test(timeOut = 30_000)
    public void testMonotonicTimestampGrowth() throws Exception {

        // Intialize component under test
        worldClockOracle.initialize();

        long last = worldClockOracle.next();
        
        int timestampIntervalSec = (int) (WorldClockOracleImpl.TIMESTAMP_INTERVAL_MS / 1000) * 2;
        for (int i = 0; i < timestampIntervalSec; i++) {
            long current = worldClockOracle.next();
            assertTrue(current > last+1 , "Timestamp should be based on world time");
            last = current;
            Thread.sleep(1000);
        }

        assertTrue(worldClockOracle.getLast() == last);
        LOG.info("Last timestamp: {}", last);
    }

    @Test(timeOut = 10_000)
    public void testTimestampOraclePanicsWhenTheStorageHasProblems() throws Exception {

        // Cause an exception when updating the max timestamp
        final CountDownLatch updateMaxTimestampMethodCalled = new CountDownLatch(1);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                updateMaxTimestampMethodCalled.countDown();
                throw new RuntimeException("Out of memory or something");
            }
        }).when(timestampStorage).updateMaxTimestamp(anyLong(), anyLong());

        // Intialize component under test
        worldClockOracle.initialize();
        updateMaxTimestampMethodCalled.await();

        // Verify that it has blown up
        verify(panicker, atLeastOnce()).panic(anyString(), any(Throwable.class));
    }

}
