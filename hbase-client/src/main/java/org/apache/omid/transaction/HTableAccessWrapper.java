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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

// This class wraps the HTableInterface object when doing client side filtering.
public class HTableAccessWrapper implements TableAccessWrapper {

    private final HTableInterface writeTable;
    private final HTableInterface readTable;
    
    public HTableAccessWrapper(HTableInterface table, HTableInterface healerTable) {
        this.readTable = table;
        this.writeTable = healerTable;
    }

    @Override
    public Result[] get(List<Get> get) throws IOException {
        return readTable.get(get);
    }

    @Override
    public Result get(Get get) throws IOException {
        return readTable.get(get);
    }

    @Override
    public void put(Put put) throws IOException {
        writeTable.put(put);
    }

}
