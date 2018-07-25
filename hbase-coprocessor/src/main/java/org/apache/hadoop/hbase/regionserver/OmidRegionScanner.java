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
import java.util.*;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.omid.transaction.SnapshotFilter;
import org.apache.omid.transaction.SnapshotFilterImpl;

public class OmidRegionScanner implements RegionScanner {

    private RegionScanner scanner;
    private SnapshotFilterImpl snapshotFilter;
    private final Queue<SnapshotFilterImpl> snapshotFilterQueue;

    public OmidRegionScanner(SnapshotFilterImpl snapshotFilter,
                             RegionScanner s, Queue<SnapshotFilterImpl> snapshotFilterQueue) {
        this.snapshotFilter = snapshotFilter;
        this.scanner = s;
        this.snapshotFilterQueue = snapshotFilterQueue;
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
       return scanner.next(results);
    }

    @Override
    public boolean next(List<Cell> list, ScannerContext scannerContext) throws IOException {
        return scanner.next(list, scannerContext);
    }

    @Override
    public void close() throws IOException {
        scanner.close();
        snapshotFilterQueue.add(snapshotFilter);
    }

    @Override
    public HRegionInfo getRegionInfo() {
        return scanner.getRegionInfo();
    }

    @Override
    public boolean isFilterDone() throws IOException {
        return scanner.isFilterDone();
    }

    @Override
    public boolean reseek(byte[] row) throws IOException {
        return scanner.reseek(row);
    }

    @Override
    public long getMaxResultSize() {
        return scanner.getMaxResultSize();
    }

    @Override
    public long getMvccReadPoint() {
        return scanner.getMvccReadPoint();
    }

    @Override
    public int getBatch() {
        return scanner.getBatch();
    }

    @Override
    public boolean nextRaw(List<Cell> result) throws IOException {
        return scanner.nextRaw(result);
    }

    @Override
    public boolean nextRaw(List<Cell> list, ScannerContext scannerContext) throws IOException {
        return scanner.nextRaw(list, scannerContext);
    }


}
