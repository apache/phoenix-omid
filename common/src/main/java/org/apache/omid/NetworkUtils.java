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
package org.apache.omid;

import java.io.IOException;
import java.net.*;
import java.util.Collections;
import java.util.Enumeration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkUtils {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkUtils.class);

    //FIXME Popular distros haven't called their network interfaces ethX for a long time
    private static final String LINUX_TSO_NET_IFACE_PREFIX = "eth";
    //Modern Linux typically uses an en prefix
    private static final String MAC_TSO_NET_IFACE_PREFIX = "en";

    // FIXME This seems to be only used for determining the host and port when registering TSO
    // to ZK for HA.
    // We should get the TSO IP from the ZK TCP connection on demand, and not worry about the
    // default network interface at all
    public static String getDefaultNetworkInterface() {

        try (DatagramSocket s=new DatagramSocket()) {
            s.connect(InetAddress.getByAddress(new byte[]{1,1,1,1}), 53);
            return NetworkInterface.getByInetAddress(s.getLocalAddress()).getName();
        } catch (Exception e) {
            //fall through
        }

        //Fall back to old logic
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            String fallBackName = null;
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface nextElement = networkInterfaces.nextElement();
                String name = nextElement.getDisplayName();
                LOG.info("Iterating over network interfaces, found '{}'", name);
                boolean hasInet = Collections.list(nextElement.getInetAddresses()).size() > 0; // Checking that inet exists, to avoid taking iBridge
                if (hasInet && fallBackName == null) {
                    fallBackName = name;
                }
                if ((name.startsWith(MAC_TSO_NET_IFACE_PREFIX) && hasInet ) ||
                        name.startsWith(LINUX_TSO_NET_IFACE_PREFIX)) {
                  return name;
                }
            }
            if (fallBackName != null) {
                return fallBackName;
            }
        } catch (SocketException ignored) {
            throw new RuntimeException("Failed to find any network interfaces", ignored);
        }

        throw new IllegalArgumentException(String.format("No network '%s*'/'%s*' interfaces found",
                                                         MAC_TSO_NET_IFACE_PREFIX, LINUX_TSO_NET_IFACE_PREFIX));
    }

    /**
     * Picks a free port on the host by binding a Socket to '0'.
     */
    public static int getFreePort() throws IOException {
        ServerSocket s = new ServerSocket(0);
        try {
            s.setReuseAddress(true);
            int port = s.getLocalPort();
            return port;
        } finally {
            if (null != s) {
                s.close();
            }
        }
    }

}
