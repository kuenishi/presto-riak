package com.basho.riak.presto;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import static com.basho.riak.presto.MetadataUtil.COLUMN_CODEC;

import javax.management.remote.rmi._RMIConnection_Stub;
import java.io.IOException;


/**
 * Created by kuenishi on 14/05/17.
 */
public class TestRiakColumnHandle {

    @Test
    public void testSerializtion2()
            throws JsonProcessingException, IOException
    {
        String connectorId = "fooo gooo";
        ColumnMetadata metadata = new ColumnMetadata("p", BooleanType.BOOLEAN, 42, false);
        RiakColumnHandle c = new RiakColumnHandle(connectorId, metadata);
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
            throws JsonProcessingException, IOException
    {
        String connectorId = "riak foo bar";
        String columnName = "DevClownVanColummmmm";

        //java.lang.String name, com.facebook.presto.spi.type.Type type, int ordinalPosition, boolean partitionKey) { /* compiled code */ }
        ColumnMetadata metadata = new ColumnMetadata(columnName, VarcharType.VARCHAR, 42, false);
        RiakColumnHandle c = new RiakColumnHandle(connectorId, metadata);

        ObjectMapper om = new ObjectMapper();
        String s = COLUMN_CODEC.toJson(c);
        System.out.println(s);

        RiakColumnHandle c2 = COLUMN_CODEC.fromJson(s);

        assert(c.equals(c2));
        assert(c.getColumn().getType() == VarcharType.VARCHAR);
        assert(c2.getColumn().getType() == VarcharType.VARCHAR);
        System.out.println(c);
        System.out.println(c2);
    }

    @Test
    public void testTupleDomain()
    {
        //TupleDomain<>
    }
}
