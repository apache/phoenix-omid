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
package org.apache.omid.tls;

/**
 * Represents a type of key pair used for X509 certs in tests. The two options are RSA or EC
 * (elliptic curve).
 * <p/>
 * This file has is based on the one in HBase project.
 * @see <a href=
 *      "https://github.com/apache/hbase/blob/d2b0074f7ad4c43d31a1a511a0d74feda72451d1/hbase-common/src/test/java/org/apache/hadoop/hbase/io/crypto/tls/X509KeyType.java">Base
 *      revision</a>
 */
public enum X509KeyType {
    RSA,
    EC
}
