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

    public void run(String[] args)
            throws IOException, InterruptedException, ExecutionException {

        RiakConfig config = new RiakConfig();
        RiakClient client = new RiakClient(config, objectMapper);
        RiakConnectorId cid = new RiakConnectorId("presto-riak-cui");

        try {
            if (args.length < 2) {
                for (String schemaName : client.getSchemaNames()) {
                    CLI.log(schemaName);
                }
                return;
            }

            String schemaName = args[1];

            Set<String> tableNames = client.getTableNames(schemaName);

            if (args.length < 3) {
                CLI.log("tables in " + schemaName);

                for (String tableName : tableNames) {
                    System.out.println(tableName);
                }
                return;
            }

            SchemaTableName schemaTableName = new SchemaTableName(schemaName, args[2]);
            RiakTable table = client.getTable(schemaTableName);
            CLI.log("table> " + table.getName());
            for (RiakColumn column : table.getColumns()) {
                String hasIndex = (column.getIndex()) ? "'" : "";
                CLI.log(column.getName() + hasIndex + ": \t" + column.getType());
            }
            //System.out.println(table);
            objectMapper.writeValue(System.out, table);
            return;
        } finally {
            client.shutdown();
        }
    }

}
