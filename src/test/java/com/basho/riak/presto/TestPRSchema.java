package com.basho.riak.presto;

import com.basho.riak.presto.models.PRSchema;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.Set;

import static com.basho.riak.presto.MetadataUtil.SCHEMA_CODEC;

/**
 * Created by kuenishi on 2014/12/5.
 */
public class TestPRSchema {
    @Test
    public void testPRSchemaSerealization()
    {
        PRSchema t = PRSchema.example();
        //System.out.println(t.toString());

        String s = SCHEMA_CODEC.toJson(t);
        //System.out.println(s);

        PRSchema t2 = SCHEMA_CODEC.fromJson(s);
        assert(t.getTables().equals(t2.getTables()));
        assert(t.getComments().equals(t2.getComments()));
    }


    public static PRSchema example() {
        Set<String> ts = Sets.newHashSet();
        Set<String> s = Sets.newHashSet("tse;lkajsdf");
        PRSchema prs = new PRSchema(ts, s);
        prs.addTable(TestPRTable.example("foobartable"), "cmd");
        return prs;
    }
}
