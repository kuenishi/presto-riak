package com.basho.riak.presto;

import com.basho.riak.presto.models.CoverageSplit;
import com.basho.riak.presto.models.PRTable;
import com.basho.riak.presto.models.RiakTableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TupleDomain;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by kuenishi on 15/05/19.
 */
public class TestSerialization {

    @Test
    public void testCoverageSplit() {
        RiakTableHandle handle = new RiakTableHandle("c", "s", "t");
        PRTable table = TestPRTable.example("boom");
        CoverageSplit coverageSplit = new CoverageSplit(handle,
                table, "192.168.0.1", "", TupleDomain.<ColumnHandle>all());
        String s = MetadataUtil.COV_CODEC.toJson(coverageSplit);

        CoverageSplit cs2 = MetadataUtil.COV_CODEC.fromJson(s);
        assert(coverageSplit.getTableHandle().getTableName()
                .equals(cs2.getTableHandle().getTableName()));
    }
}
