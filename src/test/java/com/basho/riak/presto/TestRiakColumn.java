package com.basho.riak.presto;

import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;


import static com.basho.riak.presto.MetadataUtil.COLUMN_CODEC;
/**
 * Created by kuenishi on 14/11/30.
 */
public class TestRiakColumn {
    @Test
    public void testRiakColumnCreation() throws Exception {
        String json = "{\"name\":\"foo\", \"type\":\"STRING\", \"index\":true}";
       // RiakColumn c = COLUMN_CODEC.fromJson(json);
       // assert("foo".equals(c.getName()));
       // assert(VarcharType.VARCHAR.equals(c.getType()));
       // assert(c.getIndex());
    }
}
