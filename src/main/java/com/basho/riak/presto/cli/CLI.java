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

package com.basho.riak.presto.cli;

import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.StoreOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.presto.Coverage;
import com.basho.riak.presto.DirectConnection;
import com.basho.riak.presto.RiakConfig;
import com.basho.riak.presto.SplitTask;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.*;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import static io.airlift.json.JsonBinder.jsonBinder;

/**
 * Created by kuenishi on 14/03/21.
 */
public class CLI {
    private static final Logger log = Logger.get(CLI.class);

    private static final int DATA_COUNT = 4 * 10000;

    private static final int THREAD_POOL_SIZE = 50;
    private static final int MAX_JOB_QUEUE_CAPACITY = 50;

    private static final int TOTAL_MAX_CONNECTIONS = 0; // unlimited

    private static final int MAX_CONNECTION_SIZE = 50;
    private static final int INIT_CONNECTION_SIZE = 50;

    private static final int BUFFER_KB = 1;
    private static final int IDLE_CONN_TIMEOUT_MIL = 2000;
    private static final int CONNECTION_TIMEOUT_MIL = 2000;
    private static final int REQUEST_TIMEOUT_MIL = 2000;
    private static final BinaryValue BUCKET = BinaryValue.create("test");
    private static final Namespace NAMESPACE = new Namespace(BUCKET); // with default bucket type.

    public static void usage() {
        System.out.println("presto-riak CLI. create table, create schema, drop... >");
        System.out.println("usage: ./presto-riak-cli <hostname> <port> [<commands>... ]"); //plan <node> [<cookie>])");
        System.out.println("   list-tables <schema name>");
        System.out.println("   setup-schema <schema name>");
        System.out.println("   create-tabledef <schema name> <table definition json file>");
        System.out.println("   show-tabledef <schema name> <table name>");
        System.out.println("   clear-tabledef <schema name> <table name>");
        System.out.println("   check-tabledef <schema name> <table definition json file>");

        // or mvn exec:java -q -Dexec.main=com.basho.riak.presto.CLI -Dexec.args="plan riak@127.0.0.1"
    }

    public static void main(String[] args) throws Exception {

        Injector i = Guice.createInjector(Stage.PRODUCTION, //new JsonModule());
                new Module() {
                    @Override
                    public void configure(Binder binder) {
                        jsonBinder(binder).addDeserializerBinding(Type.class).to(Deserializer.CLITypeDeserializer.class);
                        binder.bind(ObjectMapper.class).toProvider(ObjectMapperProvider.class);
                        binder.bind(JsonCodecFactory.class).in(Scopes.SINGLETON);
                    }
                });
        log.debug("%s", i.getTypeConverterBindings());

        if (args.length < 4) {
            usage();
            return;
        }

        String hostname = args[0];
        String port = args[1];
        String command = args[2];
        String schemaName = args[3];
        RiakConfig config = new RiakConfig(hostname, port);

        // Actual command implementations
        if (command.equals("list-tables") && args.length == 4) {
            new SchemaDef(i, config).listTables(schemaName);
        } else if (command.equals("setup-schema") && args.length == 4) {
            new SchemaDef(i, config).setupSchema(schemaName);

        } else if (args.length == 5) {
            String tableArg = args[4];
            if (command.equals("create-schema")) {
                CLI.log("This option is not currently supported.");
            } else if (command.equals("create-tabledef")) {
                new TableDef(i, config, schemaName, true).create(tableArg);
            } else if (command.equals("show-tabledef")) {
                new TableDef(i, config, schemaName, true).show(tableArg);
            } else if (command.equals("clear-tabledef")) {
                new TableDef(i, config, schemaName, true).clear(tableArg);
            } else if (command.equals("check-tabledef")) {
                new TableDef(i, config, schemaName, false).check(tableArg);
            }

        } else if (args[0].equals("plan")) {
            System.out.println(args[1]);

            String node = args[1];
            String cookie = "riak";
            if (args.length > 2) {
                cookie = args[2];
            }
            System.out.println("connecting to Riak node " + node + " with cookie=" + cookie);

            String self = "client@127.0.0.1";
            try {
                DirectConnection conn = new DirectConnection(self, cookie);
                conn.connect(node);
                //conn.pid();
                //conn.ping();
                Coverage coverage = new Coverage(conn);
                coverage.plan();
                List<SplitTask> splits = coverage.getSplits();

                System.out.println("print coverage plan==============");
                System.out.println(coverage.toString());

                for (SplitTask split : splits) {
                    System.out.println("============printing split data at " + split.getHost() + "===============");

                    split.fetchAllData(conn, "default", "foobartable");
                }
            } catch (java.io.IOException e) {
                System.err.println(e);
            }
        } else {
            usage();
        }
        // Mock a Wock and don't leave any trash!
        System.gc();
    }


    public static void dummy_main(String args[]) throws UnknownHostException, InterruptedException {
        System.out.println("foobar");

        RiakNode node = new RiakNode.Builder().withRemoteAddress("127.0.0.1")
                .withRemotePort(8087)
                .withMaxConnections(10)
                .withConnectionTimeout(CONNECTION_TIMEOUT_MIL)
                .build();
        List<RiakNode> nodes = Arrays.asList(node);
        RiakCluster cluster = RiakCluster.builder(nodes).build();


        long startTime = System.currentTimeMillis();

        //Bucket bucket = client.createBucket("demo_bucket").execute();
        for (int i = 0; i < DATA_COUNT; i++) {
            try {
                if (i % 1000 == 0) {
                    log("storing key #" + i);
                }
                ;
                BinaryValue key = BinaryValue.create("demo_key_" + i);
                RiakObject obj = new RiakObject();
                obj.setContentType("application/json");
                obj.setValue(BinaryValue.create("{'name':'bob','age':18}"));
                StoreOperation op = new StoreOperation.Builder(new Location(NAMESPACE, key))
                        .withContent(obj)
                        .withReturnBody(false)
                        .build();
                cluster.execute(op);

                op.await();
                if (!op.isSuccess()) {
                    throw op.cause();
                }

            } catch (Throwable t) {
                log(t.getMessage());
                log(t.getStackTrace().toString());
            }

        }

        long duration = System.currentTimeMillis() - startTime;


        log("ops per sec: " + DATA_COUNT / (duration / 1000.0));
    }

    private static void waitForQueueSizeLessThan(ThreadPoolExecutor executor, int size)
            throws InterruptedException {

        while (executor.getQueue().size() > size) {
            Thread.sleep(10L);
        }
    }

    public static void log(String log) {
        System.out.println(log);
    }

}
