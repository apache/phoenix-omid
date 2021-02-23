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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;

public final class HBaseLogin {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseLogin.class);

    private static volatile UserGroupInformation ugi;
    private static final Object KERBEROS_LOGIN_LOCK = new Object();

    @Nullable
    public static UserGroupInformation loginIfNeeded(SecureHBaseConfig config) throws IOException {
        return loginIfNeeded(config, null);
    }

    @Nullable
    public static UserGroupInformation loginIfNeeded(SecureHBaseConfig config, Configuration hbaseConf) throws IOException {
        boolean credsProvided = null != config.getPrincipal() && null != config.getKeytab();
        if (UserGroupInformation.isSecurityEnabled()) {
            // Check if we need to authenticate with kerberos so that we cache the correct ConnectionInfo
            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            if (credsProvided
                    && (!currentUser.hasKerberosCredentials()
                            || !isSameName(currentUser.getUserName(), config.getPrincipal()))) {
                synchronized (KERBEROS_LOGIN_LOCK) {
                    // Double check the current user, might have changed since we checked last. Don't want
                    // to re-login if it's the same user.
                    currentUser = UserGroupInformation.getCurrentUser();
                    if (!currentUser.hasKerberosCredentials() || !isSameName(currentUser.getUserName(), config.getPrincipal())) {
                        final Configuration hbaseConfig = getConfiguration(hbaseConf, config.getPrincipal(), config.getKeytab());
                        LOG.info("Trying to connect to a secure cluster as {} " +
                                        "with keytab {}",
                                hbaseConfig.get(SecureHBaseConfig.HBASE_CLIENT_PRINCIPAL_KEY),
                                hbaseConfig.get(SecureHBaseConfig.HBASE_CLIENT_KEYTAB_KEY));
                        UserGroupInformation.setConfiguration(hbaseConfig);
                        User.login(hbaseConfig, SecureHBaseConfig.HBASE_CLIENT_KEYTAB_KEY, SecureHBaseConfig.HBASE_CLIENT_PRINCIPAL_KEY, null);
                        LOG.info("Successful login to secure cluster");
                    }
                }
            } else {
                if (currentUser.hasKerberosCredentials()) {
                    // The user already has Kerberos creds, so there isn't anything to change in the ConnectionInfo.
                    LOG.debug("Already logged in as {}", currentUser);
                } else {
                    LOG.warn("Security enabled but not logged in, and did not provide credentials. NULL UGI returned");
                }
            }
        }
        return ugi;
    }

    static boolean isSameName(String currentName, String newName, String hostname, String defaultRealm) throws IOException {
        final boolean newNameContainsRealm = newName.indexOf('@') != -1;
        // Make sure to replace "_HOST" if it exists before comparing the principals.
        if (newName.contains(org.apache.hadoop.security.SecurityUtil.HOSTNAME_PATTERN)) {
            if (newNameContainsRealm) {
                newName = org.apache.hadoop.security.SecurityUtil.getServerPrincipal(newName, hostname);
            } else {
                // If the principal ends with "/_HOST", replace "_HOST" with the hostname.
                if (newName.endsWith("/_HOST")) {
                    newName = newName.substring(0, newName.length() - 5) + hostname;
                }
            }
        }
        // The new name doesn't contain a realm and we could compute a default realm
        if (!newNameContainsRealm && defaultRealm != null) {
            return currentName.equals(newName + "@" + defaultRealm);
        }
        // We expect both names to contain a realm, so we can do a simple equality check
        return currentName.equals(newName);
    }

    static boolean isSameName(String currentName, String newName) throws IOException {
        return isSameName(currentName, newName, null, getDefaultKerberosRealm());
    }

    /**
     * Computes the default kerberos realm if one is available. If one cannot be computed, null
     * is returned.
     *
     * @return The default kerberos realm, or null.
     */
    static String getDefaultKerberosRealm() {
        try {
            return KerberosUtil.getDefaultRealm();
        } catch (Exception e) {
            return null;
        }
    }

    private static Configuration getConfiguration(Configuration conf, String principal, String keytab) {
        if(conf == null) {
            conf = HBaseConfiguration.create();
        }
        // Set the principal and keytab if provided from the URL (overriding those provided in Properties)
        if (null != principal) {
            conf.set(SecureHBaseConfig.HBASE_CLIENT_PRINCIPAL_KEY, principal);
        }
        if (null != keytab) {
            conf.set(SecureHBaseConfig.HBASE_CLIENT_KEYTAB_KEY, keytab);
        }
        return conf;
    }
}
