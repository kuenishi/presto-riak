package com.basho.riak.presto;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.BooleanType;
import org.junit.Test;

import static com.basho.riak.presto.MetadataUtil.TABLE_CODEC;

/**
 * Created by kuenishi on 14/12/03.
 */
public class TestRiakTable {

    @Test
    public void testSerialization()
    {

        RiakTable t = RiakTable.example("foobar_table");
        System.out.println(t.toString());

        String s = TABLE_CODEC.toJson(t);
        System.out.println(s);

        RiakTable t2 = TABLE_CODEC.fromJson(s);
        System.out.println(t2.toString());
        System.out.println(t2.getName());
        assert(t.getName().equals(t2.getName()));
        assert(t.getColumns().equals(t2.getColumns()));
    }
}
