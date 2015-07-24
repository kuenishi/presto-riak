package com.basho.riak.presto;

import com.basho.riak.presto.models.RiakColumn;
import com.basho.riak.presto.models.RiakColumnHandle;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static com.basho.riak.presto.MetadataUtil.COLUMN_CODEC;

import java.io.IOException;


/**
 * Created by kuenishi on 14/05/17.
 */
public class TestRiakColumnHandle {

    @Test
    public void testSerializtion2()
            throws IOException
    {
        String connectorId = "fooo gooo";
        RiakColumn column = new RiakColumn("p", BooleanType.BOOLEAN, "boom", true, false);
        RiakColumnHandle c = new RiakColumnHandle(connectorId, column, 4);
        assert(c.getColumn().getType() == BooleanType.BOOLEAN);

        String s = COLUMN_CODEC.toJson(c);
        System.out.println(s);

        RiakColumnHandle c2 = COLUMN_CODEC.fromJson(s);

        assert(c.equals(c2));
        assert(c.getColumn().getType() == BooleanType.BOOLEAN);
        assert(c2.getColumn().getType() == BooleanType.BOOLEAN);
        System.out.println(c);
        System.out.println(c2);
    }

    @Test
    public void testSerialization()
            throws IOException
    {
        String connectorId = "riak foo bar";
        String columnName = "DevClownVanColummmmm";

        RiakColumn column = new RiakColumn(columnName, VarcharType.VARCHAR, "comment, ...", true, false);
        RiakColumnHandle c = new RiakColumnHandle(connectorId, column, 54);

        ObjectMapper om = new ObjectMapper();
        String s = COLUMN_CODEC.toJson(c);

        RiakColumnHandle c2 = COLUMN_CODEC.fromJson(s);

        assert(c.equals(c2));
        assert(c.getColumn().getType() == VarcharType.VARCHAR);
        assert(c2.getColumn().getType() == VarcharType.VARCHAR);
    }

    @Test
    public void testTupleDomain()
    {
        //TupleDomain<>
    }
}
