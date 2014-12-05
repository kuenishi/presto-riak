package com.basho.riak.presto;

import org.junit.Test;

import static com.basho.riak.presto.MetadataUtil.SCHEMA_CODEC;

/**
 * Created by kuenishi on 2014/12/5.
 */
public class TestPRSchema {
    @Test
    public void testPRSchemaSerealization()
    {
        PRSchema t = PRSchema.example();
        System.out.println(t.toString());

        String s = SCHEMA_CODEC.toJson(t);
        System.out.println(s);

        PRSchema t2 = SCHEMA_CODEC.fromJson(s);
        assert(t.getTables().equals(t2.getTables()));
        assert(t.getComments().equals(t2.getComments()));
    }
}
