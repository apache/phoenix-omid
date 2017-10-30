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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.omid.transaction.TableAccessWrapper;

public class RegionAccessWrapper implements TableAccessWrapper {

    private final Region region;
    
    public RegionAccessWrapper(Region region) {
        this.region = region;
    }

    @Override
    public Result[] get(List<Get> get) throws IOException {
        Result[] results = new Result[get.size()];

        int i = 0;
        for (Get g : get) {
            results[i++] = region.get(g);
        }
        return results;
    }

    @Override
    public Result get(Get get) throws IOException {
        return region.get(get);
    }

    @Override
    public void put(Put put) throws IOException {
        region.put(put);
    }

}
