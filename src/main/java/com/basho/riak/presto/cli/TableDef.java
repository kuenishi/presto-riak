package com.basho.riak.presto.cli;

import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.presto.*;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by kuenishi on 14/12/04.
 */
public class TableDef {

    private final ObjectMapper objectMapper;
    private final String schemaName;
    private final RiakConfig config;
    private final RiakConnectorId cid;
    private RiakClient client = null;

    TableDef(Injector i, RiakConfig config,
             String schemaName, boolean doConnect)
            throws IOException, InterruptedException {
        this.objectMapper = i.getInstance(ObjectMapper.class);
        this.schemaName = schemaName;

        this.config = checkNotNull(config);
        if (doConnect) {
            client = new RiakClient(config, objectMapper);
        } else {
            client = null;
        }
        cid = new RiakConnectorId("presto-riak-cui");
    }

    @Override
    protected void finalize() throws Throwable {
        if (client != null)
            client.shutdown();
    }

    public boolean check(String filename)
            throws FileNotFoundException, IOException {
        PRTable table = readTable(filename);
        // because there were no exceptions thrown, that's ok
        CLI.log("successfully loaded: " + table.toString());
        return true;
    }


    // @doc this does not do any read before write
    public boolean create(String filename)
            throws FileNotFoundException, IOException, InterruptedException, ExecutionException {

        PRTable table = readTable(filename);
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, table.getName());

        List<RiakObject> objects = client.getSchemaRiakObjects(schemaTableName.getSchemaName());
        for (RiakObject o : objects) {
            CLI.log(o.getValue().toStringUtf8());
            PRSchema schema = objectMapper.readValue(o.getValue().toStringUtf8(), PRSchema.class);

            schema.addTable(table, "added today");
            o.setValue(BinaryValue.create(objectMapper.writeValueAsBytes(schema)));

            if (client.storeSchema(schemaName, o)) {
                if( client.storeTable(schemaTableName, table)){
                    CLI.log("Table " + schemaTableName + " successfully created.");
                }
            }
        }
        return false;
    }

    public boolean show(String tableName)
            throws InterruptedException, ExecutionException, IOException {
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        // TODO: check siblings and do warn!
        PRTable table = client.getTable(schemaTableName);
        CLI.log("table> " + table.getName());
        for (RiakColumn column : table.getColumns()) {
            String hasIndex = (column.getIndex()) ? "'" : "";
            CLI.log(column.getName() + hasIndex + ": \t" + column.getType());
        }
        //System.out.println(table);
        objectMapper.writeValue(System.out, table);
        return true;

    }

    public boolean clear(String tableName) {
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        return client.deleteTable(schemaTableName);
    }

    // only JSON files accepted
    private PRTable readTable(String filename)
            throws FileNotFoundException, IOException {
        FileReader reader = new FileReader(filename);
        return objectMapper.readValue(reader, PRTable.class);
    }
}
