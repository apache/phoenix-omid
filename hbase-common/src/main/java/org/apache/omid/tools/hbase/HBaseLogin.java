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

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;

public final class HBaseLogin {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseLogin.class);

    private static volatile UserGroupInformation ugi;

    @Nullable
    public static UserGroupInformation loginIfNeeded(SecureHBaseConfig config) throws IOException {

        if (UserGroupInformation.isSecurityEnabled()) {
            LOG.info("Security enabled when connecting to HBase");
            if (ugi == null) { // Use lazy initialization with double-checked locking
                synchronized (HBaseLogin.class) {
                    if (ugi == null) {
                        LOG.info("Login with Kerberos. User={}, keytab={}", config.getPrincipal(), config.getKeytab());
                        UserGroupInformation.loginUserFromKeytab(config.getPrincipal(), config.getKeytab());
                        ugi = UserGroupInformation.getCurrentUser();
                    }
                }
            } else {
                LOG.info("User {}, already trusted (Kerberos). Avoiding 2nd login as it causes problems", ugi.toString());
            }
        } else {
            LOG.warn("Security NOT enabled when connecting to HBase. Act at your own risk. NULL UGI returned");
        }
        return ugi;
    }

}
