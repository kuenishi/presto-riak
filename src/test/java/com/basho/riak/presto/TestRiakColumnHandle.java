package com.basho.riak.presto;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

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
        assert(c.getColumn().spiType() == BooleanType.BOOLEAN);

        ObjectMapper om = new ObjectMapper();
        byte[] b = om.writeValueAsBytes(c);

        System.out.println(om.writeValueAsString(c));

        RiakColumnHandle c2 = om.readValue(b, RiakColumnHandle.class);

        assert(c.equals(c2));
        assert(c.getColumn().spiType() == BooleanType.BOOLEAN);
        assert(c2.getColumn().spiType() == BooleanType.BOOLEAN);
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
        byte[] b = om.writeValueAsBytes(c);

        System.out.println(om.writeValueAsString(c));

        RiakColumnHandle c2 = om.readValue(b, RiakColumnHandle.class);

        assert(c.equals(c2));
        assert(c.getColumn().spiType() == VarcharType.VARCHAR);
        assert(c2.getColumn().spiType() == VarcharType.VARCHAR);
        System.out.println(c);
        System.out.println(c2);
    }
}
