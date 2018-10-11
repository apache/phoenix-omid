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
package org.apache.hadoop.hbase.ipc.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.DelegatingHBaseRpcController;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;

import com.google.protobuf.RpcController;

/**
 * {@link RpcController} that sets the appropriate priority of RPC calls destined for Phoenix index
 * tables.
 */
class InterRegionServerRpcController extends DelegatingHBaseRpcController {
    private final int priority;
    
    public InterRegionServerRpcController(HBaseRpcController delegate, Configuration conf) {
        super(delegate);
        // Set priority higher that normal, but lower than high
        this.priority = (HConstants.HIGH_QOS + HConstants.NORMAL_QOS) / 2;
    }
    
    @Override
    public void setPriority(final TableName tn) {
        if (tn.isSystemTable()) {
            super.setPriority(tn);
        } else {
            setPriority(this.priority);
        }
    }
    

}