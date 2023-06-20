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
package org.apache.omid.benchmarks.tso;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.ScheduledReporter;
import com.google.inject.Module;
import org.apache.omid.benchmarks.utils.ScrambledZipfianGenerator;
import org.apache.omid.benchmarks.utils.UniformGenerator;
import org.apache.omid.benchmarks.utils.ZipfianGenerator;
import org.apache.omid.metrics.CodahaleMetricsProvider;
import org.apache.omid.metrics.MetricsRegistry;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.apache.omid.committable.hbase.DefaultHBaseCommitTableStorageModule;
import org.apache.omid.tso.client.OmidClientConfiguration;

import static org.apache.omid.tso.client.OmidClientConfiguration.ConnType.DIRECT;

import java.util.List;

public class TSOServerBechmarkConfigTest {

    @Test(timeOut = 10_000)
    public void testParsesOK() throws Exception {
        TSOServerBenchmarkConfig tsoServerBenchmarkConfig = new TSOServerBenchmarkConfig();

        Assert.assertEquals(tsoServerBenchmarkConfig.getBenchmarkRunLengthInMins(), 5);
        Assert.assertEquals(tsoServerBenchmarkConfig.getTxRunners(), 1);
        Assert.assertEquals(tsoServerBenchmarkConfig.getTxRateInRequestPerSecond(), 100);
        Assert.assertEquals(tsoServerBenchmarkConfig.getWarmUpPeriodInSecs(), 30);
        Assert.assertEquals(tsoServerBenchmarkConfig.isFixedWritesetSize(), true);
        Assert.assertEquals(tsoServerBenchmarkConfig.getPercentageOfReadOnlyTxs(), 0);
        Assert.assertEquals(tsoServerBenchmarkConfig.getCommitDelayInMs(), 0);

        Assert.assertEquals(tsoServerBenchmarkConfig.getCellIdGenerator().getClass(), UniformGenerator.class);

        Assert.assertEquals(tsoServerBenchmarkConfig.getCommitTableStoreModule().getClass(), DefaultHBaseCommitTableStorageModule.class);
        DefaultHBaseCommitTableStorageModule ctstm = (DefaultHBaseCommitTableStorageModule) tsoServerBenchmarkConfig.getCommitTableStoreModule();
        Assert.assertEquals("OMID_COMMIT_TABLE", ctstm.getTableName());

        Assert.assertEquals(tsoServerBenchmarkConfig.getMetrics().getClass(), CodahaleMetricsProvider.class);

        MetricsRegistry cmp = tsoServerBenchmarkConfig.getMetrics();
        Assert.assertTrue(cmp instanceof CodahaleMetricsProvider);
        CodahaleMetricsProvider codahaleMetricsProvider = (CodahaleMetricsProvider) cmp;
        Assert.assertEquals(10, codahaleMetricsProvider.getMetricsOutputFrequencyInSecs());
        List<ScheduledReporter> reporters = codahaleMetricsProvider.getReporters();
        Assert.assertTrue(reporters.get(0) instanceof CsvReporter);

        OmidClientConfiguration omidClientConf = tsoServerBenchmarkConfig.getOmidClientConfiguration();
        Assert.assertEquals(omidClientConf.getConnectionType(), DIRECT);
        Assert.assertEquals(omidClientConf.getConnectionString(), "localhost:54758");
    }

    @Test(timeOut = 10_000)
    public void testParsesCustom() throws Exception {
        TSOServerBenchmarkConfig tsoServerBenchmarkConfig = new TSOServerBenchmarkConfig("tso-server-benchmark-config2.yml");

        Assert.assertEquals(tsoServerBenchmarkConfig.getBenchmarkRunLengthInMins(), 5);
        Assert.assertEquals(tsoServerBenchmarkConfig.getTxRunners(), 1);
        Assert.assertEquals(tsoServerBenchmarkConfig.getTxRateInRequestPerSecond(), 100);
        Assert.assertEquals(tsoServerBenchmarkConfig.getWarmUpPeriodInSecs(), 30);
        Assert.assertEquals(tsoServerBenchmarkConfig.isFixedWritesetSize(), true);
        Assert.assertEquals(tsoServerBenchmarkConfig.getPercentageOfReadOnlyTxs(), 0);
        Assert.assertEquals(tsoServerBenchmarkConfig.getCommitDelayInMs(), 0);

        Assert.assertEquals(tsoServerBenchmarkConfig.getCellIdGenerator().getClass(), ZipfianGenerator.class);
        ZipfianGenerator gen = (ZipfianGenerator) tsoServerBenchmarkConfig.getCellIdGenerator();
        Assert.assertEquals(gen.getItems(), 50000L);


        Assert.assertEquals(tsoServerBenchmarkConfig.getCommitTableStoreModule().getClass(), DefaultHBaseCommitTableStorageModule.class);
        DefaultHBaseCommitTableStorageModule ctstm = (DefaultHBaseCommitTableStorageModule) tsoServerBenchmarkConfig.getCommitTableStoreModule();
        Assert.assertEquals("OMID_COMMIT_TABLE", ctstm.getTableName());

        Assert.assertEquals(tsoServerBenchmarkConfig.getMetrics().getClass(), CodahaleMetricsProvider.class);

        MetricsRegistry cmp = tsoServerBenchmarkConfig.getMetrics();
        Assert.assertTrue(cmp instanceof CodahaleMetricsProvider);
        CodahaleMetricsProvider codahaleMetricsProvider = (CodahaleMetricsProvider) cmp;
        Assert.assertEquals(10, codahaleMetricsProvider.getMetricsOutputFrequencyInSecs());
        List<ScheduledReporter> reporters = codahaleMetricsProvider.getReporters();
        Assert.assertTrue(reporters.get(0) instanceof CsvReporter);

        OmidClientConfiguration omidClientConf = tsoServerBenchmarkConfig.getOmidClientConfiguration();
        Assert.assertEquals(omidClientConf.getConnectionType(), DIRECT);
        Assert.assertEquals(omidClientConf.getConnectionString(), "localhost:54758");
    }
}