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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.ScannerContext;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class HBaseShims {

    static public void setKeyValueSequenceId(KeyValue kv, int sequenceId) {

        kv.setSequenceId(sequenceId);

    }

    static public RegionWrapper getRegionCoprocessorRegion(RegionCoprocessorEnvironment env) {

        return new RegionWrapper(env.getRegion());

    }

    static public void flushAllOnlineRegions(HRegionServer regionServer, TableName tableName) throws IOException {

        for (Region r : regionServer.getOnlineRegions(tableName)) {
            r.flush(true);
        }

    }

    static public void addFamilyToHTableDescriptor(HTableDescriptor tableDesc, HColumnDescriptor columnDesc) {

        tableDesc.addFamily(columnDesc);

    }

    static public int getBatchLimit(ScannerContext scannerContext) throws IOException {

        // Invoke scannerContext.getBatchLimit() through reflection as is not accessible in HBase 1.x version
        try {
            return (int) ReflectionHelper.invokeParameterlessMethod(scannerContext, "getBatchLimit");
        } catch (NoSuchMethodException e) {
            throw new IOException("Can't find getBatchLimit method in ScannerContext through reflection", e);
        } catch (IllegalAccessException e) {
            throw new IOException("Can't access getBatchLimit method in ScannerContext through reflection", e);
        } catch (InvocationTargetException e) {
            throw new IOException("Exception thrown in calling getBatchLimit method through reflection", e);
        }

    }

}
