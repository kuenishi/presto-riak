package com.basho.riak.presto;

import org.junit.Test;

import java.util.LinkedList;
import com.facebook.presto.spi.type.Type;
import static com.basho.riak.presto.MetadataUtil.OUTPUT_TABLE_CODEC;

/**
 * Created by kuenishi on 2015/2/4.
 */
public class TestOutputTableHandle  {

  @Test
                                      public void testSerialization()
{

    RiakOutputTableHandle t = new RiakOutputTableHandle(
            "connid", "schema", "table", new LinkedList<String>(),
            new LinkedList<Type>());
    System.out.println(t.toString());

    String s = OUTPUT_TABLE_CODEC.toJson(t);
    System.out.println(s);

    RiakOutputTableHandle t2 = OUTPUT_TABLE_CODEC.fromJson(s);
    System.out.println(t2.toString());
    System.out.println(t2.getColumnNames());
    assert(t.getColumnNames().equals(t2.getColumnNames()));
    assert(t.getColumnTypes().equals(t2.getColumnTypes()));
}
}
