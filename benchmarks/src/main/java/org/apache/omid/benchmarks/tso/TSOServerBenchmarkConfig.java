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

import com.google.inject.AbstractModule;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.omid.YAMLUtils;
import org.apache.omid.benchmarks.utils.IntegerGenerator;
import org.apache.omid.metrics.CodahaleMetricsConfig;
import org.apache.omid.metrics.CodahaleMetricsProvider;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.tools.hbase.SecureHBaseConfig;
import org.apache.omid.tso.client.OmidClientConfiguration;
import org.apache.omid.tso.InMemoryCommitTableStorageModule;
import org.apache.omid.committable.hbase.DefaultHBaseCommitTableStorageModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;

public class TSOServerBenchmarkConfig extends SecureHBaseConfig {

    private static final Logger LOG = LoggerFactory.getLogger(TSOServerBenchmarkConfig.class);

    private static final String CONFIG_FILE_NAME = "tso-server-benchmark-config.yml";
    private static final String DEFAULT_CONFIG_FILE_NAME = "default-tso-server-benchmark-config.yml";

    private long benchmarkRunLengthInMins;

    private int txRunners;
    private int txRateInRequestPerSecond;
    private long warmUpPeriodInSecs;
    private IntegerGenerator cellIdGenerator;
    private int writesetSize;
    private boolean fixedWritesetSize;
    private int percentageOfReadOnlyTxs;
    private long commitDelayInMs;

    private OmidClientConfiguration omidClientConfiguration;
    private AbstractModule commitTableStoreModule;

    private MetricsRegistry metrics;

    // ----------------------------------------------------------------------------------------------------------------
    // Instantiation
    // ----------------------------------------------------------------------------------------------------------------

    TSOServerBenchmarkConfig() {
        this(CONFIG_FILE_NAME);
    }

    TSOServerBenchmarkConfig(String configFileName) {
        Map props = new YAMLUtils().getSettingsMap(configFileName, DEFAULT_CONFIG_FILE_NAME);
        populateProperties(props);
    }

    public void populateProperties(Map props){
        try {
            Object idGen;
            Map idGenProps = (Map) props.get("cellIdGenerator");
            long maxID = Long.MAX_VALUE;
            if (idGenProps != null && idGenProps.containsKey("max")) {
                String val = String.valueOf(idGenProps.get("max"));
                if (val.endsWith("L")) {
                    val = val.substring(0, val.length() - 1);
                }
                try {
                    maxID = Long.parseLong(val);
                } catch (Exception e) {
                    LOG.debug(String.valueOf(e));
                    LOG.debug(Arrays.toString(e.getStackTrace()));
                    System.out.println(Arrays.toString(e.getStackTrace()));
                    try {
                        maxID = Long.decode(val);
                    } catch (Exception ex) {
                        LOG.debug(String.valueOf(ex));
                        LOG.debug(Arrays.toString(ex.getStackTrace()));
                        maxID = Long.MAX_VALUE;
                        LOG.info("Failed to convert max number for cellIdGenerator, using Long.MAX_VALUE instead");
                    }
                }
            }

            if (idGenProps != null && idGenProps.containsKey("class") &&
                    "org.apache.omid.benchmarks.utils.ZipfianGenerator".equals(idGenProps.get("class"))) {
                idGen = new org.apache.omid.benchmarks.utils.ZipfianGenerator(maxID);
                idGenProps.remove("class");
            } else if (idGenProps != null && idGenProps.containsKey("class") &&
                    "org.apache.omid.benchmarks.utils.ScrambledZipfianGenerator".equals(idGenProps.get("class"))) {
                idGen = new org.apache.omid.benchmarks.utils.ScrambledZipfianGenerator(maxID);
                idGenProps.remove("class");
            } else {
                idGen = new org.apache.omid.benchmarks.utils.UniformGenerator();
            }
            props.put("cellIdGenerator", idGen);

            if (props.containsKey("omidClientConfiguration")) {
                Object omidClientConf = new OmidClientConfiguration();
                Map omidClientProps = (Map<String, ? extends Object>) props.get("omidClientConfiguration");
                if (omidClientProps != null && omidClientProps.containsKey("connectionType")) {
                    omidClientProps.put("connectionType", org.apache.omid.tso.client.OmidClientConfiguration.ConnType.valueOf((String) omidClientProps.get("connectionType")));
                }
                if (omidClientProps != null && omidClientProps.containsKey("postCommitMode")) {
                    omidClientProps.put("postCommitMode", org.apache.omid.tso.client.OmidClientConfiguration.PostCommitMode.valueOf((String) omidClientProps.get("postCommitMode")));
                }
                if (omidClientProps != null && omidClientProps.containsKey("conflictDetectionLevel")) {
                    omidClientProps.put("conflictDetectionLevel", org.apache.omid.tso.client.OmidClientConfiguration.ConflictDetectionLevel.valueOf((String) omidClientProps.get("conflictDetectionLevel")));
                }
                BeanUtils.populate((Object) omidClientConf, omidClientProps);
                props.put("omidClientConfiguration", omidClientConf);
            }

            Object ctsm;
            Map ctsmProps = (Map) props.get("commitTableStoreModule");
            if (ctsmProps != null && ctsmProps.containsKey("class") &&
                    "org.apache.omid.committable.hbase.DefaultHBaseCommitTableStorageModule".equals(ctsmProps.get("class"))) {
                ctsm = new DefaultHBaseCommitTableStorageModule();
                ctsmProps.remove("class");
            } else {
                ctsm = new InMemoryCommitTableStorageModule();
            }
            BeanUtils.populate((Object) ctsm, (Map<String, ? extends Object>) ctsmProps);
            props.put("commitTableStoreModule", ctsm);

            Object mp;
            Map mpProps = (Map) props.get("metrics");
            if (mpProps != null && mpProps.containsKey("class") &&
                    "org.apache.omid.metrics.CodahaleMetricsProvider".equals(mpProps.get("class"))) {
                CodahaleMetricsConfig mmc = new CodahaleMetricsConfig();
                mpProps.remove("class");
                Set reporters = new HashSet<>();
                for (Object r : (ArrayList) mpProps.get("reporters")) {
                    reporters.add(CodahaleMetricsConfig.Reporter.valueOf((String) r));
                }
                mpProps.put("reporters", reporters);

                BeanUtils.populate((Object) mmc, (Map<String, ? extends Object>) mpProps);
                mp = new CodahaleMetricsProvider(mmc);
            } else {
                mp = new NullMetricsProvider();
            }
            props.put("metrics", mp);

            BeanUtils.populate(this, props);
        } catch (IllegalAccessException | InvocationTargetException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Getters and setters for config params
    // ----------------------------------------------------------------------------------------------------------------

    public long getBenchmarkRunLengthInMins() {
        return benchmarkRunLengthInMins;
    }

    public void setBenchmarkRunLengthInMins(long benchmarkRunLengthInMins) {
        this.benchmarkRunLengthInMins = benchmarkRunLengthInMins;
    }

    public int getTxRunners() {
        return txRunners;
    }

    public void setTxRunners(int txRunners) {
        this.txRunners = txRunners;
    }

    public int getTxRateInRequestPerSecond() {
        return txRateInRequestPerSecond;
    }

    public void setTxRateInRequestPerSecond(int txRateInRequestPerSecond) {
        this.txRateInRequestPerSecond = txRateInRequestPerSecond;
    }

    public long getWarmUpPeriodInSecs() {
        return warmUpPeriodInSecs;
    }

    public void setWarmUpPeriodInSecs(long warmUpPeriodInSecs) {
        this.warmUpPeriodInSecs = warmUpPeriodInSecs;
    }

    public IntegerGenerator getCellIdGenerator() {
        return cellIdGenerator;
    }

    public void setCellIdGenerator(IntegerGenerator cellIdGenerator) {
        this.cellIdGenerator = cellIdGenerator;
    }

    public int getWritesetSize() {
        return writesetSize;
    }

    public void setWritesetSize(int writesetSize) {
        this.writesetSize = writesetSize;
    }

    public boolean isFixedWritesetSize() {
        return fixedWritesetSize;
    }

    public void setFixedWritesetSize(boolean fixedWritesetSize) {
        this.fixedWritesetSize = fixedWritesetSize;
    }

    public int getPercentageOfReadOnlyTxs() {
        return percentageOfReadOnlyTxs;
    }

    public void setPercentageOfReadOnlyTxs(int percentageOfReadOnlyTxs) {
        this.percentageOfReadOnlyTxs = percentageOfReadOnlyTxs;
    }

    public long getCommitDelayInMs() {
        return commitDelayInMs;
    }

    public void setCommitDelayInMs(long commitDelayInMs) {
        this.commitDelayInMs = commitDelayInMs;
    }

    public OmidClientConfiguration getOmidClientConfiguration() {
        return omidClientConfiguration;
    }

    public void setOmidClientConfiguration(OmidClientConfiguration omidClientConfiguration) {
        this.omidClientConfiguration = omidClientConfiguration;
    }

    public AbstractModule getCommitTableStoreModule() {
        return commitTableStoreModule;
    }

    public void setCommitTableStoreModule(AbstractModule commitTableStoreModule) {
        this.commitTableStoreModule = commitTableStoreModule;
    }

    public MetricsRegistry getMetrics() {
        return metrics;
    }

    public void setMetrics(MetricsRegistry metrics) {
        this.metrics = metrics;
    }

}
