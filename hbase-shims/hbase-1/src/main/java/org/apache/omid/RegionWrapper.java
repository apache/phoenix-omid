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

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.Region;

import java.io.IOException;

/**
 * Wrapper over {@link org.apache.hadoop.hbase.regionserver.Region} interface in HBase 1.x versions
 */
public class RegionWrapper {

    Region region;

    public RegionWrapper(Region region) {

        this.region = region;

    }

    public Result get(Get getOperation) throws IOException {

        return region.get(getOperation);

    }

    public HRegionInfo getRegionInfo() {

        return region.getRegionInfo();

    }

}