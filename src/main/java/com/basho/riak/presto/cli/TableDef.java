package com.basho.riak.presto.cli;

import com.basho.riak.presto.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by kuenishi on 14/12/04.
 */
public class TableDef {

    private final ObjectMapper objectMapper;
    private final String schemaName;
    private final RiakConfig config;
    private final RiakClient client;
    private final RiakConnectorId cid;

    TableDef(Injector i, String schemaName)
            throws IOException, InterruptedException
    {
        this.objectMapper = i.getInstance(ObjectMapper.class);
        this.schemaName = schemaName;

        config = new RiakConfig();
        client = new RiakClient(config, objectMapper);
        cid = new RiakConnectorId("presto-riak-cui");
    }

    public boolean check(String filename)
            throws FileNotFoundException, IOException
    {
        FileReader reader = new FileReader(filename);
        RiakTable table = objectMapper.readValue(reader, RiakTable.class);
        return true;
    }

    public boolean create(String filename) {
        return false;
    }

    public boolean clear(String tableName) {
        return false;
    }
}
