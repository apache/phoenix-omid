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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.Timer;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.omid.metrics.MetricsUtils.name;

public class LowWatermarkWriterImpl implements LowWatermarkWriter {

    private static final Logger LOG = LoggerFactory.getLogger(LowWatermarkWriterImpl.class);

    private final Timer lwmWriteTimer;
    private final CommitTable.Writer lowWatermarkWriter;
    private final ExecutorService lowWatermarkWriterExecutor;
    private MetricsRegistry metrics;

    @Inject
    LowWatermarkWriterImpl(TSOServerConfig config,
                           CommitTable commitTable,
                           MetricsRegistry metrics)
            throws Exception {
        this.metrics = metrics;
        this.lowWatermarkWriter = commitTable.getWriter();
        // Low Watermark writer
        ThreadFactoryBuilder lwmThreadFactory = new ThreadFactoryBuilder().setNameFormat("lwm-writer-%d");
        this.lowWatermarkWriterExecutor = Executors.newSingleThreadExecutor(lwmThreadFactory.build());

        // Metrics config
        this.lwmWriteTimer = metrics.timer(name("tso", "lwmWriter", "latency"));
        LOG.info("PersistentProcessor initialized");
    }

    @Override
    public Future<Void> persistLowWatermark(final long lowWatermark) {

        return lowWatermarkWriterExecutor.submit(new Callable<Void>() {
            @Override
            public Void call() throws IOException {
                try {
                    lwmWriteTimer.start();
                    lowWatermarkWriter.updateLowWatermark(lowWatermark);
                    lowWatermarkWriter.flush();
                } finally {
                    lwmWriteTimer.stop();
                }
                return null;
            }
        });
    }
}
