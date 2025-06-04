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
package org.apache.omid.timestamp.storage;

import com.google.inject.AbstractModule;

/**
 * This class is instantiated by the yaml parser.
 * Snake_yaml needs a public POJO style class to work properly with all the setters and getters.
 */
public class DefaultZKTimestampStorageModule extends AbstractModule {

    private String zkCluster = "localhost:2181";
    private String namespace = "omid";
    private String zkLoginContextName;

    @Override
    public void configure() {
        install(new ZKModule(zkCluster, namespace, zkLoginContextName));
        install(new ZKTimestampStorageModule());
    }

    // ----------------------------------------------------------------------------------------------------------------
    // WARNING: Do not remove getters/setters, needed by snake_yaml!
    // ----------------------------------------------------------------------------------------------------------------

    public String getZkCluster() {
        return zkCluster;
    }

    public void setZkCluster(String zkCluster) {
        this.zkCluster = zkCluster;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getZkLoginContextName() {
        return zkLoginContextName;
    }

    public void setZkLoginContextName(String zkLoginContextName) {
        this.zkLoginContextName = zkLoginContextName;
    }

}
