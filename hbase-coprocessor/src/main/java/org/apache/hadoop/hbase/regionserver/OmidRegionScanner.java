package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.omid.committable.CommitTable.Client;

public class OmidRegionScanner implements RegionScanner{

    
    OmidRegionScanner(ObserverContext<RegionCoprocessorEnvironment> e,
                      InternalScanner internalScanner,
                      Client commitTableClient) {
        
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean next(List<Cell> result, int limit) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public HRegionInfo getRegionInfo() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isFilterDone() throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean reseek(byte[] row) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public long getMaxResultSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getMvccReadPoint() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean nextRaw(List<Cell> result) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean nextRaw(List<Cell> result, int limit) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

}
