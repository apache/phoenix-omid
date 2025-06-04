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
package org.apache.omid.tools.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.hadoop.hbase.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Programmatic Jaas configuration object, which adds a new app configuration called
 * _omid_kerberos_ticket_cache, and falls back to the Configuration in effect when this was called.
 * To be used for connecting to Zookeeper, to reuse the kerberos ticket cache set by Hadoop/HBase
 * Based on Hadoop's JaasConfiguration
 */
public class OmidKerberosTicketCacheConfiguration extends Configuration {

    private static final Logger LOG =
            LoggerFactory.getLogger(OmidKerberosTicketCacheConfiguration.class);

    private final javax.security.auth.login.Configuration baseConfig =
            javax.security.auth.login.Configuration.getConfiguration();

    private final AppConfigurationEntry[] entry;

    public static String APP_NAME = "_omid_kerberos_ticket_cache";

    public OmidKerberosTicketCacheConfiguration() {
        super();
        Map<String, String> options = new HashMap<>();
        options.put("useTicketCache", "true");
        options.putAll(getPrincipalIfNeeded());
        entry =
                new AppConfigurationEntry[] { new AppConfigurationEntry(getKrb5LoginModuleName(),
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options) };
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        return (APP_NAME.equals(name)) ? entry
                : ((baseConfig != null) ? baseConfig.getAppConfigurationEntry(name) : null);
    }

    private String getKrb5LoginModuleName() {
        String krb5LoginModuleName;
        if (System.getProperty("java.vendor").contains("IBM")) {
            krb5LoginModuleName = "com.ibm.security.auth.module.Krb5LoginModule";
        } else {
            krb5LoginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
        }
        return krb5LoginModuleName;
    }

    private Map<String, String> getPrincipalIfNeeded() {
        HashMap<String, String> principalOptions = new HashMap<>();
        if (System.getProperty("java.vendor").contains("IBM")) {
            // The IBM Kerberos implementation does not pick up the cache entry without the
            // principal
            String principalName;
            try {
                principalName = User.getCurrent().getName();
                if (principalName == null || principalName.isEmpty()) {
                    LOG.warn("Got empty principal name on IMB JVM");
                } else {
                    principalOptions.put("principal", principalName);
                }
            } catch (IOException e) {
                LOG.warn("Could not determine principal on IBM JVM.", e);
            }

        }
        return principalOptions;
    }

}
