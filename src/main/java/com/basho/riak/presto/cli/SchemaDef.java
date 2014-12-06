package com.basho.riak.presto.cli;

import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.presto.PRSchema;
import com.basho.riak.presto.RiakClient;
import com.basho.riak.presto.RiakConfig;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import io.airlift.log.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import static com.google.common.base.Preconditions.checkNotNull;


/**
 * Created by kuenishi on 14/12/04.
 */
public class SchemaDef {
    private static final Logger log = Logger.get(SchemaDef.class);
    private final RiakConfig config;
    private ObjectMapper objectMapper;
    private RiakClient client;

    public SchemaDef(Injector i, RiakConfig config) {
        objectMapper = i.getInstance(ObjectMapper.class);
        this.config = checkNotNull(config);
    }

    public static boolean delTable(RiakClient client, SchemaTableName schemaTableName) {
        return false;
    }

    private void setupClient(RiakConfig config)
            throws IOException, InterruptedException, ExecutionException {
        client = new RiakClient(config, objectMapper);
    }

    public void setupSchema(String schemaName) {
        try {
            setupClient(config);
            List<RiakObject> objects = client.getSchemaRiakObjects(schemaName);
            if (objects.size() > 0) {
                CLI.log("Schema is already up: " + schemaName);
                return;
            }

            // NOTE: there can be an interleaved schema creation here
            // and as a consequence, there are multiple siblings. At any
            // moment, presto-riak does not handle siblings in metadata.
            // Just picks up the first sibling.
            PRSchema schema = new PRSchema(new HashSet<String>(), new HashSet<String>());

            if (!client.storeSchema(schemaName, schema)) {
                CLI.log("failed creating schema");
            }
            CLI.log("success: " + schemaName);

        } catch (Exception e) {
            log.error(e);
        } finally {
            client.shutdown();
        }
    }

    public void listTables(String schemaName) {
        try {
            setupClient(config);
            Set<String> tableNames = client.getTableNames(schemaName);
            CLI.log("tables in " + schemaName);

            for (String tableName : tableNames) {
                System.out.println(tableName);
            }
        } catch (Exception e) {
            log.error(e);
        } finally {
            client.shutdown();
        }
    }
}
