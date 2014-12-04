package com.basho.riak.presto.cli;

import com.basho.riak.presto.*;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Created by kuenishi on 14/12/04.
 */
public class SchemaDef {

    private ObjectMapper objectMapper;

    public SchemaDef(Injector i) {
        objectMapper = i.getInstance(ObjectMapper.class);
    }

    public void setupSchema(String schemaName)
    {
        // void
    }
    public void listTables(String schemaName)
            throws IOException, InterruptedException, ExecutionException {

        RiakConfig config = new RiakConfig();
        RiakClient client = new RiakClient(config, objectMapper);
        RiakConnectorId cid = new RiakConnectorId("presto-riak-cui");

        try {

            Set<String> tableNames = client.getTableNames(schemaName);

            CLI.log("tables in " + schemaName);

            for (String tableName : tableNames) {
                System.out.println(tableName);
            }
        } finally
        {
            client.shutdown();
        }
    }

}
