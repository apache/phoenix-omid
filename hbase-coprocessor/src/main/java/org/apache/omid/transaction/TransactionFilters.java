package org.apache.omid.transaction;

import com.beust.jcommander.internal.Nullable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.RegionAccessWrapper;

import java.util.Queue;

public class TransactionFilters {

    public static Filter getVisibilityFilter(@Nullable Filter cellFilter,
                                             SnapshotFilterImpl regionAccessWrapper,
                                             HBaseTransaction hbaseTransaction) {
        return new TransactionVisibilityFilter(cellFilter, regionAccessWrapper, hbaseTransaction);
    }
}
