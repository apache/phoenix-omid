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

import org.junit.Assert;
import org.testng.annotations.Test;

import org.apache.omid.tso.client.OmidClientConfiguration;

import static org.apache.omid.tso.client.OmidClientConfiguration.ConnType.HA;

public class TestHBaseOmidClientConfiguration {

    @Test(timeOut = 10_000)
    public void testYamlReading() {
        HBaseOmidClientConfiguration configuration = new HBaseOmidClientConfiguration();
        Assert.assertNotNull(configuration.getCommitTableName());
        Assert.assertNotNull(configuration.getHBaseConfiguration());
        Assert.assertNotNull(configuration.getMetrics());
        Assert.assertNotNull(configuration.getOmidClientConfiguration());
    }

    @Test(timeOut = 10_000)
    public void testYamlReadingFromFile() {
        HBaseOmidClientConfiguration configuration = new HBaseOmidClientConfiguration("/test-hbase-omid-client-config.yml");
        Assert.assertNotNull(configuration.getCommitTableName());
        Assert.assertNotNull(configuration.getHBaseConfiguration());
        Assert.assertNotNull(configuration.getMetrics());
        OmidClientConfiguration omidClientConf = configuration.getOmidClientConfiguration();
        Assert.assertNotNull(omidClientConf);

        org.testng.Assert.assertEquals(omidClientConf.getConnectionType(), HA);
        org.testng.Assert.assertEquals(omidClientConf.getConnectionString(), "somehost:54758");
        org.testng.Assert.assertEquals(omidClientConf.getZkConnectionTimeoutInSecs(), 11);
        org.testng.Assert.assertEquals(omidClientConf.getRequestMaxRetries(), 6);
        org.testng.Assert.assertEquals(omidClientConf.getExecutorThreads(), 4);

        Assert.assertEquals(1, 2);
    }

}