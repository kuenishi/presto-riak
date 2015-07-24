package com.basho.riak.presto;

import com.basho.riak.presto.models.PRSubTable;
import com.basho.riak.presto.models.PRTable;
import com.basho.riak.presto.models.RiakColumn;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarcharType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.basho.riak.presto.MetadataUtil.TABLE_CODEC;

/**
 * Created by kuenishi on 14/12/03.
 */
public class TestPRTable {

    @Test
    public void testSerialization()
    {
        PRTable t;
        t = example("foobar_table");

        String s = TABLE_CODEC.toJson(t);
        PRTable t2 = TABLE_CODEC.fromJson(s);
        assert(t.getName().equals(t2.getName()));
        assert(t.getColumns().equals(t2.getColumns()));
        assert(t.getSubtable("foobar_table/boom") != null);
        assert(t.getSubtable("foobar_table/boom").getName() == "boom");
        assert(t.getSubtable("foobar_table/noop") == null);
    }

    @Test
    public void testDeSerialization()
    {
        //TODO: this is strange to me that with empty subtables added test passes,
        // while without subtables, null, fails
        String s = "{\"comment\":\"comme\", \"subtables\":[],\"name\":\"logs\", \"columns\":[{\"name\":\"a\", \"type\":\"varchar\", \"index\":false, \"pkey\":true}]}";
        PRTable t = TABLE_CODEC.fromJson(s);
        assert(t.getName().equals("logs"));
        assert(t.getSubtables().isEmpty());
        assert(null != t.getColumnHandles("a"));
    }

    public static PRTable example(String tableName) {

        List<RiakColumn> cols = Arrays.asList(
                new RiakColumn("col1", VarcharType.VARCHAR, "d1vv", false, true),
                new RiakColumn("col2", VarcharType.VARCHAR, "d2", true, false),
                new RiakColumn("poopie", BigintType.BIGINT, "d3", true, false));
        String subTableName = "boom";
        String subTablePath = "$.col1[*]";
        List<PRSubTable> subtables = Arrays.asList(subTableExample(subTableName, subTablePath));
        return new PRTable(tableName, cols, "comment", subtables);
    }

    static PRSubTable subTableExample(String tableName, String path)
    {
        List<RiakColumn> cols = Arrays.asList(
                new RiakColumn("col1", VarcharType.VARCHAR, "d1vv", false, true),
                new RiakColumn("col2", VarcharType.VARCHAR, "d2", true, false),
                new RiakColumn("poopie", BigintType.BIGINT, "d3", true, false));
        PRSubTable st = new PRSubTable(tableName, cols, path);
        return st;
    }
}
