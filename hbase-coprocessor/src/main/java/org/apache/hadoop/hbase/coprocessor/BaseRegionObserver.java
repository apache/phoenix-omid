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
package org.apache.hadoop.hbase.coprocessor;

import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import java.io.IOException;
import java.util.Optional;


public abstract class BaseRegionObserver implements RegionObserver, RegionCoprocessor {
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c,
                                      Store store,
                                      InternalScanner scanner,
                                      ScanType scanType,
                                      CompactionLifeCycleTracker tracker,
                                      CompactionRequest request) throws IOException {
        return preCompact(c,store,scanner,scanType,request);
    }

    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> env,
                                      Store store,
                                      InternalScanner scanner,
                                      ScanType scanType,
                                      CompactionRequest request) throws IOException {
        return scanner;
    }

    @Override
    public Optional getRegionObserver() {
        return Optional.of(this);
    }

}
