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

import com.google.inject.Inject;
import org.apache.omid.metrics.MetricsRegistry;
import org.jboss.netty.channel.Channel;

import java.io.IOException;

public class RequestProcessorPersistCT extends AbstractRequestProcessor {

    PersistenceProcessor persistenceProcessor;

    @Inject
    RequestProcessorPersistCT(MetricsRegistry metrics,
                              TimestampOracle timestampOracle,
                              PersistenceProcessor persistenceProcessor,
                              Panicker panicker,
                              TSOServerConfig config,
                              LowWatermarkWriter lowWatermarkWriter) throws IOException {

        super(metrics, timestampOracle, panicker, config, lowWatermarkWriter);
        this.persistenceProcessor = persistenceProcessor;
        requestRing = disruptor.start();
    }



    @Override
    public void forwardCommit(long startTimestamp, long commitTimestamp, Channel c, MonitoringContext monCtx) throws Exception {
        persistenceProcessor.addCommitToBatch(startTimestamp,commitTimestamp,c,monCtx);
    }

    @Override
    public void forwardCommitRetry(long startTimestamp, Channel c, MonitoringContext monCtx) throws Exception {
        persistenceProcessor.addCommitRetryToBatch(startTimestamp,c,monCtx);
    }

    @Override
    public void forwardAbort(long startTimestamp, Channel c, MonitoringContext monCtx) throws Exception {
        persistenceProcessor.addAbortToBatch(startTimestamp,c,monCtx);
    }

    @Override
    public void forwardTimestamp(long startTimestamp, Channel c, MonitoringContext monCtx) throws Exception {
        persistenceProcessor.addTimestampToBatch(startTimestamp,c,monCtx);
    }

    @Override
    public void onTimeout() throws Exception {
        persistenceProcessor.triggerCurrentBatchFlush();
    }
}
