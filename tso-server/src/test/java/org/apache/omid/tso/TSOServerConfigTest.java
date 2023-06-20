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

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.ScheduledReporter;
import com.google.inject.Module;
import org.apache.omid.metrics.CodahaleMetricsProvider;
import org.apache.omid.metrics.MetricsRegistry;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.apache.omid.timestamp.storage.DefaultHBaseTimestampStorageModule;
import org.apache.omid.committable.hbase.DefaultHBaseCommitTableStorageModule;

import java.util.List;

public class TSOServerConfigTest {

    @Test(timeOut = 10_000)
    public void testParsesOK() throws Exception {
        TSOServerConfig tsoServerConfig = new TSOServerConfig("test-omid.yml");
        Assert.assertEquals(tsoServerConfig.getTlsEnabled(), false);
        Assert.assertEquals(tsoServerConfig.getSupportPlainText(), true);
        Assert.assertEquals(tsoServerConfig.getEnabledProtocols(), null);
        Assert.assertEquals(tsoServerConfig.getTsConfigProtocols(), "TLSv1.2");
        Assert.assertEquals(tsoServerConfig.getKeyStoreLocation(), "");
        Assert.assertEquals(tsoServerConfig.getKeyStorePassword(), "");
        Assert.assertEquals(tsoServerConfig.getKeyStoreType(), "");
        Assert.assertEquals(tsoServerConfig.getTrustStoreLocation(), "");
        Assert.assertEquals(tsoServerConfig.getTrustStorePassword(), "");
        Assert.assertEquals(tsoServerConfig.getKeyStoreType(), "");

        Assert.assertEquals(tsoServerConfig.getTimestampStoreModule().getClass(), DefaultHBaseTimestampStorageModule.class);
        DefaultHBaseTimestampStorageModule tsm = (DefaultHBaseTimestampStorageModule) tsoServerConfig.getTimestampStoreModule();

        Assert.assertEquals("sieve_omid:OMID_COMMIT_TABLE_F", tsm.getTableName());
        Assert.assertEquals("MAX_TIMESTAMP_F", tsm.getFamilyName());

        Assert.assertEquals(tsoServerConfig.getCommitTableStoreModule().getClass(), DefaultHBaseCommitTableStorageModule.class);
        DefaultHBaseCommitTableStorageModule ctstm = (DefaultHBaseCommitTableStorageModule) tsoServerConfig.getCommitTableStoreModule();
        Assert.assertEquals("sieve_omid:OMID_TIMESTAMP_F", ctstm.getTableName());
    }

    @Test(timeOut = 10_000)
    public void testCustomParseK() throws Exception {
        TSOServerConfig tsoServerConfig = new TSOServerConfig("tlstest-omid-server-config.yml");
        Assert.assertEquals(tsoServerConfig.getCipherSuites(), "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA");
        Assert.assertEquals(tsoServerConfig.getTlsEnabled(), true);
        Assert.assertEquals(tsoServerConfig.getSupportPlainText(), false);

        Assert.assertEquals(tsoServerConfig.getEnabledProtocols(), "TLSv1.2");
        Assert.assertEquals(tsoServerConfig.getTsConfigProtocols(), "TLSv1.2");

        Assert.assertEquals(tsoServerConfig.getKeyStoreLocation(), "/asd");
        Assert.assertEquals(tsoServerConfig.getKeyStorePassword(), "pass");
        Assert.assertEquals(tsoServerConfig.getKeyStoreType(), "JKS");
        Assert.assertEquals(tsoServerConfig.getTrustStoreLocation(), "/tasd");
        Assert.assertEquals(tsoServerConfig.getTrustStorePassword(), "tpas$w0rd1");
        Assert.assertEquals(tsoServerConfig.getKeyStoreType(), "JKS");
    }

    @Test(timeOut = 10_000)
    public void testCustomStorageModule() throws Exception {
        TSOServerConfig tsoServerConfig = new TSOServerConfig("custom-omid-server-config.yml");


        Assert.assertEquals(tsoServerConfig.getCipherSuites(), "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA");
        Assert.assertEquals(tsoServerConfig.getTlsEnabled(), true);
        Assert.assertEquals(tsoServerConfig.getSupportPlainText(), false);

        Assert.assertEquals(tsoServerConfig.getEnabledProtocols(), "TLSv1.2");
        Assert.assertEquals(tsoServerConfig.getTsConfigProtocols(), "TLSv1.2");

        Assert.assertEquals(tsoServerConfig.getKeyStoreLocation(), "/asd");
        Assert.assertEquals(tsoServerConfig.getKeyStorePassword(), "pass");
        Assert.assertEquals(tsoServerConfig.getKeyStoreType(), "JKS");
        Assert.assertEquals(tsoServerConfig.getTrustStoreLocation(), "/tasd");
        Assert.assertEquals(tsoServerConfig.getTrustStorePassword(), "tpas$w0rd1");
        Assert.assertEquals(tsoServerConfig.getKeyStoreType(), "JKS");

        Assert.assertEquals(tsoServerConfig.getTimestampStoreModule().getClass(), DefaultHBaseTimestampStorageModule.class);
        Assert.assertEquals(tsoServerConfig.getCommitTableStoreModule().getClass(), DefaultHBaseCommitTableStorageModule.class);

        Assert.assertEquals(tsoServerConfig.getLeaseModule().getClass(), HALeaseManagementModule.class);
        HALeaseManagementModule lm = (HALeaseManagementModule) tsoServerConfig.getLeaseModule();
        Assert.assertEquals(lm.getCurrentTsoPath(), "/custom-current-tso");
        Assert.assertEquals(lm.getTsoLeasePath(), "/custom-tso-lease");
        Assert.assertEquals(lm.getTsoLeasePath(), "/custom-tso-lease");
        Assert.assertEquals(lm.getZkCluster(), "custom-host");
        Assert.assertEquals(lm.getZkNamespace(), "custom-omid");


        Assert.assertEquals(tsoServerConfig.getMetrics().getClass(), CodahaleMetricsProvider.class);

        MetricsRegistry cmp = tsoServerConfig.getMetrics();
        Assert.assertTrue(cmp instanceof CodahaleMetricsProvider);
        CodahaleMetricsProvider codahaleMetricsProvider = (CodahaleMetricsProvider) cmp;
        Assert.assertEquals(60, codahaleMetricsProvider.getMetricsOutputFrequencyInSecs());
        List<ScheduledReporter> reporters = codahaleMetricsProvider.getReporters();
        if (reporters.get(0) instanceof CsvReporter) {
            Assert.assertTrue(reporters.get(1) instanceof ConsoleReporter);
        } else if (reporters.get(1) instanceof CsvReporter) {
            Assert.assertTrue(reporters.get(0) instanceof ConsoleReporter);
        }
        else {
            Assert.assertTrue(false);
        }
    }

    @Test(timeOut = 10_000)
    public void testCustom() throws Exception {
        TSOServerConfig tsoServerConfig = new TSOServerConfig("test-omid-server-config-indentation.yml");
        Assert.assertEquals(tsoServerConfig.getCipherSuites(), "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA");
        Assert.assertEquals(tsoServerConfig.getTlsEnabled(), true);
        Assert.assertEquals(tsoServerConfig.getSupportPlainText(), false);

        Assert.assertEquals(tsoServerConfig.getEnabledProtocols(), "TLSv1.2");
        Assert.assertEquals(tsoServerConfig.getTsConfigProtocols(), "TLSv1.2");

        Assert.assertEquals(tsoServerConfig.getKeyStoreLocation(), "/asd");
        Assert.assertEquals(tsoServerConfig.getKeyStorePassword(), "pass");
        Assert.assertEquals(tsoServerConfig.getKeyStoreType(), "JKS");
        Assert.assertEquals(tsoServerConfig.getTrustStoreLocation(), "/tasd");
        Assert.assertEquals(tsoServerConfig.getTrustStorePassword(), "tpas$w0rd1");
        Assert.assertEquals(tsoServerConfig.getKeyStoreType(), "JKS");

        Assert.assertEquals(tsoServerConfig.getLeaseModule().getClass(), HALeaseManagementModule.class);
        HALeaseManagementModule lm = (HALeaseManagementModule) tsoServerConfig.getLeaseModule();
        Assert.assertEquals(lm.getCurrentTsoPath(), "/custom-current-tso");
    }


}