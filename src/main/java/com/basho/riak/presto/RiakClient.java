/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.presto;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.BucketProperties;
import com.basho.riak.pbc.RiakObject;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.google.common.net.HostAndPort;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.*;

import static com.basho.riak.presto.RiakTable.nameGetter;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;

import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.basho.riak.client.IRiakClient;

import com.basho.riak.client.bucket.Bucket;
import io.airlift.log.Logger;

public class RiakClient
{
    private static final int THREAD_POOL_SIZE = 50;
    private static final int MAX_JOB_QUEUE_CAPACITY = 50;

    private static final int TOTAL_MAX_CONNECTIONS = 0; // unlimited

    private static final int MAX_CONNECTION_SIZE = 50;
    private static final int INIT_CONNECTION_SIZE = 50;

    private static final int BUFFER_KB = 1;
    private static final int IDLE_CONN_TIMEOUT_MIL = 2000;
    private static final int CONNECTION_TIMEOUT_MIL = 2000;
    private static final int REQUEST_TIMEOUT_MIL = 2000;

    public static final String META_BUCKET_NAME = "__presto_schema";
    public static final String SCHEMA_KEY_NAME = "__schema";

    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final List<String> schemas;
    private final PBClusterConfig clusterConfig;
    private final String hosts;

    private static final Logger log = Logger.get(RiakClient.class);

    @Inject
    public RiakClient(RiakConfig config) //}, JsonCodec<Map<String, List<RiakTable>>> catalogCodec)
            throws IOException
    {
        checkNotNull(config, "config is null");

        this.hosts = checkNotNull(config.getHost());
        log.info("Riak Config: %s", hosts);

        HostAndPort hp = HostAndPort.fromString(hosts);
//		PBClientConfig node1 = PBClientConfig.defaults();

        PBClientConfig node1 = new PBClientConfig.Builder()
                .withHost(hp.getHostText())
                .withPort(hp.getPortOrDefault(8087))
                .withConnectionTimeoutMillis(CONNECTION_TIMEOUT_MIL)
                .withIdleConnectionTTLMillis(IDLE_CONN_TIMEOUT_MIL)
                .withSocketBufferSizeKb(BUFFER_KB)
                .withRequestTimeoutMillis(REQUEST_TIMEOUT_MIL)
                .withInitialPoolSize(INIT_CONNECTION_SIZE)
                .withPoolSize(MAX_CONNECTION_SIZE)
                .build();

        clusterConfig = new PBClusterConfig(TOTAL_MAX_CONNECTIONS);
        clusterConfig.addClient(node1);

        //final String hosts = config.getHosts();
        this.schemas = Arrays.asList("default");
        //String json = Resources.toString(metadataUri.toURL(), Charsets.UTF_8);
        //Map<String, List<RiakTable>> catalog = catalogCodec.fromJson(json);
        //this.schemas = ImmutableMap.copyOf(transformValues(catalog, resolveAndIndexTables(metadataUri)));
    }

    public Set<String> getSchemaNames()
    {
        // TODO: fetch all bucket types from somewhere,
        // maybe from configuration or a key that stores metadata
        return new HashSet<String>(this.schemas);
    }

    public Set<String> getTableNames(String schema)
    {
        log.info("checking... rawDatabaseaaaaa %s", schema);
        IRiakClient client = null;
        try{
            client = RiakFactory.newClient(clusterConfig);
            Bucket bucket = client.createBucket(META_BUCKET_NAME).execute();

            RawDatabase rawDatabase = null;
            String schemaKey = schema;
            if(schema.equals("default")){
                schemaKey = SCHEMA_KEY_NAME;
            }

            // null return if not found
            IRiakObject riakObject = bucket.fetch(schemaKey).execute();

            //log.debug(riakObject.toString());
            //log.debug(riakObject.getValueAsString());

            //log.debug("checking... rawDatabase");
            rawDatabase = bucket.fetch(schemaKey, RawDatabase.class).execute();

            checkNotNull(rawDatabase, "no schema key exists in Riak");
            //if(rawDatabase == null) log.debug("rawDatabase is null");
            //else                    log.debug("rawDatabase is not null");

            checkNotNull(rawDatabase.tables, "bad schema that doesn't have no table property");
            //log.debug("%s tables found for schema %s", rawDatabase.tables.size(), schema);

            return new HashSet<String>(rawDatabase.tables);
        }
        catch (RiakException e)
        {
            log.error(e);
        }
        finally{
            if(client != null) client.shutdown();
        }
        Set<String> s = new HashSet<String>(Arrays.asList("foobartable"));
        return ImmutableSet.copyOf(s);
    }

    public RiakTable getTable(String schema, String tableName)
    {
        checkNotNull(schema, "schema is null");
        checkNotNull(tableName, "tableName is null");

        log.info("RiakClient.getTable(%s, %s)", schema, tableName);
        //Map<String, RiakTable> tables = schemas.get(schema);
        //if (tables == null) {
        //    return null;
        IRiakClient client = null;
        try{
            client = RiakFactory.newClient(clusterConfig);
            Bucket bucket = client.createBucket(META_BUCKET_NAME).execute();

            String tableKey = schema + "." + tableName;
            log.debug("foobar;-");
            RiakTable table = bucket.fetch(tableKey, RiakTable.class).execute();

            log.debug("foobar;--");
            checkNotNull(table, "table schema (%s) wasn't found.", tableKey);
            log.debug("table %s schema found.", tableName);

            return table;
        }
        catch (RiakException e)
        {
            log.error(e);
            return null;
        }
        finally{
            if(client != null) client.shutdown();
        }
    }

    public List<RiakTable> getTables(String schema)
    {
        IRiakClient client = null;
        try{
            client = RiakFactory.newClient(clusterConfig);
            Bucket bucket = client.createBucket(META_BUCKET_NAME).execute();

            RawDatabase rawDatabase = bucket.fetch(SCHEMA_KEY_NAME, RawDatabase.class).execute();
            log.debug("%d tables found for schema %s", rawDatabase.tables.size(),
                    schema);

            List<RiakTable> riakTableList = new LinkedList<RiakTable>();
            for (String table : rawDatabase.tables)
            {
                log.info("table %s found.", table);
                BucketProperties bucketProperties = client.fetchBucket(table).execute();
                log.debug(bucketProperties.toString());
                //RiakTable tableObject = bucketProperties.toString();
            }
            return riakTableList;
        }
        catch (RiakException e)
        {
            log.error(e);
        }
        finally{
            if(client != null) client.shutdown();
        }
        return null;
    }

    public String getHosts(){ return hosts; }

    private static Function<URI, URI> uriResolver(final URI baseUri)
    {
        return new Function<URI, URI>()
        {
            @Override
            public URI apply(URI source)
            {
                return baseUri.resolve(source);
            }
        };
    }
}
