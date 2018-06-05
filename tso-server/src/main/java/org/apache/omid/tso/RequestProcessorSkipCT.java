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

public class RequestProcessorSkipCT extends AbstractRequestProcessor {


    ReplyProcessor replyProcessor;

    @Inject
    RequestProcessorSkipCT(MetricsRegistry metrics,
                           TimestampOracle timestampOracle,
                           ReplyProcessor replyProcessor,
                           Panicker panicker,
                           TSOServerConfig config,
                           LowWatermarkWriter lowWatermarkWriter) throws IOException {
        super(metrics, timestampOracle, panicker, config, lowWatermarkWriter);
        this.replyProcessor = replyProcessor;
        requestRing = disruptor.start();
    }

    @Override
    public void forwardCommit(long startTimestamp, long commitTimestamp, Channel c, MonitoringContext monCtx) {
        replyProcessor.sendCommitResponse(startTimestamp, commitTimestamp, c);
    }

    @Override
    public void forwardCommitRetry(long startTimestamp, Channel c, MonitoringContext monCtx) {
        replyProcessor.sendAbortResponse(startTimestamp, c);
    }

    @Override
    public void forwardAbort(long startTimestamp, Channel c, MonitoringContext monCtx) {
        replyProcessor.sendAbortResponse(startTimestamp, c);
    }

    @Override
    public void forwardTimestamp(long startTimestamp, Channel c, MonitoringContext monCtx) {
        replyProcessor.sendTimestampResponse(startTimestamp, c);
    }

    @Override
    public void onTimeout() {
        
    }
}
