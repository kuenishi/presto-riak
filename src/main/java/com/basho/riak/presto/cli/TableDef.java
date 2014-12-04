package com.basho.riak.presto.cli;

import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.presto.*;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.sun.tools.internal.ws.wsdl.document.jaxws.Exception;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Created by kuenishi on 14/12/04.
 */
public class TableDef {

    private final ObjectMapper objectMapper;
    private final String schemaName;
    private final RiakConfig config;
    private final RiakClient client;
    private final RiakConnectorId cid;

    TableDef(Injector i, String schemaName, boolean doConnect)
            throws IOException, InterruptedException
    {
        this.objectMapper = i.getInstance(ObjectMapper.class);
        this.schemaName = schemaName;

        config = new RiakConfig();
        if(doConnect) {
            client = new RiakClient(config, objectMapper);
        }else{
            client = null;
        }
        cid = new RiakConnectorId("presto-riak-cui");
    }

    public boolean check(String filename)
            throws FileNotFoundException, IOException
    {
        RiakTable table = readTable(filename);
        // because there were no exceptions thrown, that's ok
        CLI.log("successfully loaded: "+ table.toString());
        return true;
    }

    // @doc this does not do any read before write
    public boolean create(String filename)
        throws FileNotFoundException, IOException, InterruptedException
    {
        try {
            RiakTable table = readTable(filename);
            if (table != null)
            {
                CLI.log("There are existing table. Delete it before putting new table definition.");
            }
            SchemaTableName schemaTableName = new SchemaTableName(schemaName, table.getName());
            return client.storeTable(schemaTableName, table);
        }finally
        {
            client.shutdown();
        }
    }

    public boolean show(String tableName)
            throws InterruptedException, ExecutionException, IOException
    {
        try {
            SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
            RiakTable table = client.getTable(schemaTableName);
            CLI.log("table> " + table.getName());
            for (RiakColumn column : table.getColumns()) {
                String hasIndex = (column.getIndex()) ? "'" : "";
                CLI.log(column.getName() + hasIndex + ": \t" + column.getType());
            }
            //System.out.println(table);
            objectMapper.writeValue(System.out, table);
            return true;
        }finally
        {
            client.shutdown();
        }
    }

    public boolean clear(String tableName)
    {
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        return client.deleteTable(schemaTableName);
    }

    // only JSON files accepted
    private RiakTable readTable(String filename)
        throws FileNotFoundException, IOException
    {
        FileReader reader = new FileReader(filename);
        return objectMapper.readValue(reader, RiakTable.class);
    }
}
