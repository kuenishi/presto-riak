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


import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.airlift.log.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;

public class RiakClient {

    public static final String META_BUCKET_NAME = "__presto_schema";
    public static final String SCHEMA_KEY_NAME = "__schema";
    public static final Namespace NAMESPACE = new Namespace(META_BUCKET_NAME);
    private static final int CONNECTION_TIMEOUT_MIL = 2000;
    private static final Logger log = Logger.get(RiakClient.class);
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final List<String> schemas;
    private final RiakCluster cluster;
    private final String hosts;
    private final RiakConfig config;

    private ObjectMapper objectMapper = null;

    @Inject
    public RiakClient(RiakConfig config, ObjectMapper objectMapper) //}, JsonCodec<Map<String, List<RiakTable>>> catalogCodec)
            throws IOException, InterruptedException {
        this.config = checkNotNull(config, "config is null");
        this.objectMapper = checkNotNull(objectMapper, "om is null");

        this.hosts = checkNotNull(config.getHost());
        log.info("Riak Config: %s", hosts);

        HostAndPort hp = HostAndPort.fromString(hosts);
//		PBClientConfig node1 = PBClientConfig.defaults();

        RiakNode node = new RiakNode.Builder()
                .withRemoteAddress(hp.getHostText())
                .withRemotePort(hp.getPortOrDefault(8087))
                .withMaxConnections(10)
                .withConnectionTimeout(CONNECTION_TIMEOUT_MIL)
                .build();
        cluster = RiakCluster.builder(Arrays.asList(node)).build();

        //final String hosts = config.getHosts();
        this.schemas = Arrays.asList("md", "t");
        //String json = Resources.toString(metadataUri.toURL(), Charsets.UTF_8);
        //Map<String, List<RiakTable>> catalog = catalogCodec.fromJson(json);
        //this.schemas = ImmutableMap.copyOf(transformValues(catalog, resolveAndIndexTables(metadataUri)));
        cluster.start();
        // insert your names;
        // TODO: how do we unregister when presto node shuts down?
        register();
    }

    private static Function<URI, URI> uriResolver(final URI baseUri) {
        return new Function<URI, URI>() {
            @Override
            public URI apply(URI source) {
                return baseUri.resolve(source);
            }
        };
    }

    // @doc register presto node's hostname and port to Riak,
    // so as to Riak can return correct presto node corresponding to a vnode.
    private void register() throws InterruptedException {

        String host = HostAndPort.fromString(config.getHost()).getHostText();
        log.debug("presto port ===> %s:%s", host, config.getPrestoPort());
        // riak.erlang.node => { presto.erlang.node, node.ip, http-server.http.port }
        PairwiseNode pairNode = new PairwiseNode(config.getLocalNode(), host, config.getPrestoPort());
        RiakObject obj = new RiakObject();
        obj.setContentType("application/json");
        obj.setValue(BinaryValue.create(pairNode.toString()));
        log.debug("Registering membership: %s", pairNode.toString());

        log.info("localnode: %s", config.getLocalNode());
        BinaryValue localNode = BinaryValue.create(config.getLocalNode());
        StoreOperation op = new StoreOperation.Builder(new Location(NAMESPACE, localNode))
                .withContent(obj).build();

        cluster.execute(op);

        op.await();
        if (op.isSuccess()) {
            log.info("membership registered: %s => %s:%s",
                    config.getLocalNode(), pairNode.getHost(), pairNode.getPort());
        } else {
            log.error("failed to register membership");
        }
    }

    public Set<String> getSchemaNames() {
        // TODO: fetch all bucket types from somewhere,
        // maybe from configuration or a key that stores metadata
        return new HashSet<String>(this.schemas);
    }

    public Set<String> getTableNames(String schemaName) throws InterruptedException, ExecutionException, IOException {
        log.info("checking... rawDatabase %s\n", schemaName);


        // null return if not found
        FetchOperation op = buildFetchOperation(schemaName, META_BUCKET_NAME, SCHEMA_KEY_NAME);
        cluster.execute(op);

        op.await();
        if (!op.isSuccess()) {
            return null;
        }

        List<RiakObject> objects = op.get().getObjectList();
        Map<String, Map<String, Object>> data;

        ObjectMapper om = new ObjectMapper();
        for (RiakObject o : objects) {

            log.debug(o.toString());
            log.debug(o.getValue().toStringUtf8());

            data = om.readValue(o.getValue().toStringUtf8(), Map.class);
            log.debug("Got schema: %s", data.toString());

            checkNotNull(data, "no schema key exists in Riak");
            //if(rawDatabase == null) log.debug("rawDatabase is null");
            //else                    log.debug("rawDatabase is not null");

            checkNotNull(data.get("tables"), "bad schema that doesn't have no table property");
            //log.debug("%s tables found for schema %s", rawDatabase.tables.size(), schema);
            HashSet<String> set = new HashSet<>();
            return ImmutableSet.copyOf(data.get("tables").keySet());
        }
        Set<String> s = new HashSet<String>();
        return ImmutableSet.copyOf(s);
    }

    public RiakTable getTable(SchemaTableName schemaTableName)
            throws InterruptedException, ExecutionException, IOException {
        checkNotNull(schemaTableName, "tableName is null");

        log.info("RiakClient.getTable(%s)", schemaTableName);
        //Map<String, RiakTable> tables = schemas.get(schema);
        //if (tables == null) {
        //    return null;


        FetchOperation op = buildFetchOperation(
                schemaTableName.getSchemaName(),
                META_BUCKET_NAME, schemaTableName.getTableName());

        cluster.execute(op);
        op.await();
        if (!op.isSuccess()) {
            return null;
        }

        //ObjectMapper om = new ObjectMapper();
        List<RiakObject> objects = op.get().getObjectList();
        for (RiakObject o : objects) {
            log.debug("ro: %s", o.getValue().toStringUtf8());
            RiakTable table = objectMapper.readValue(o.getValue().toStringUtf8(), RiakTable.class);
            checkNotNull(table, "table schema (%s) wasn't found.", schemaTableName.getSchemaName());
            log.debug("table %s schema found.", schemaTableName.getTableName());

            return table;
        }
        return null;
    }

    public String getHosts() {
        return hosts;
    }

    private FetchOperation buildFetchOperation(String bucketType, String bucket, String key){
        Namespace namespace = new Namespace(bucketType, bucket);
        return new FetchOperation.Builder(new Location(namespace, key)).build();

    }

    public void shutdown()
    {
        cluster.shutdown();
    }
}
