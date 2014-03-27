package com.basho.riak.presto;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.DefaultRetrier;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.facebook.presto.spi.Split;

/**
 * Created by kuenishi on 14/03/21.
 */
public class CLI {

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

    public static void main(String[] args) throws Exception
    {
        System.out.println(args[0]);
        System.out.println(args[1]);
        //System.out.println(args[2]);

        if(args.length == 0) {
            System.out.println("presto-riak CLI. create table, create schema, drop... >");
            System.out.println("usage: ./presto-riak-cli plan <node> [<cookie>]");
            // or mvn exec:java -q -Dexec.main=com.basho.riak.presto.CLI -Dexec.args="plan riak@127.0.0.1"
            return;
        }

        if(args[0].equals("plan")) {
            String node = args[1];
            String cookie = "riak";
            if(args.length > 2){  cookie = args[2]; }
            System.out.println("connecting to Riak node "+node+" with cookie="+cookie);

            String self = "client@127.0.0.1";
            try {
                DirectConnection conn = new DirectConnection(self, cookie);
                conn.connect(node);
                //conn.ping();
                Coverage coverage = new Coverage(conn);
                coverage.plan();
                List<SplitTask> splits = coverage.getSplits();

                System.out.println("print coverage plan==============");
                System.out.println(coverage.toString());

                for(SplitTask split : splits)
                {
                    System.out.println("============printing split data at "+split.getHost()+"===============");

                    split.fetchAllData(conn, "default", "foobartable");
                }
            }
            catch (java.io.IOException e){
                System.err.println(e);
            }
        }
    }

    public static void dummy_main(String args[]) throws RiakException, RiakRetryFailedException, InterruptedException {
        System.out.println("foobar");

        PBClientConfig node1 = new PBClientConfig.Builder()
                .withHost("127.0.0.1")
                .withPort(8087)
                .withConnectionTimeoutMillis(CONNECTION_TIMEOUT_MIL)
                .withIdleConnectionTTLMillis(IDLE_CONN_TIMEOUT_MIL)
                .withSocketBufferSizeKb(BUFFER_KB)
                .withRequestTimeoutMillis(REQUEST_TIMEOUT_MIL)
                .withInitialPoolSize(INIT_CONNECTION_SIZE)
                .withPoolSize(MAX_CONNECTION_SIZE)
                .build();

        PBClientConfig node2 = PBClientConfig.Builder.from(node1)
                .withHost("127.0.0.1")
                .withPort(8087)
                .build();

        PBClusterConfig clusterConf = new PBClusterConfig(TOTAL_MAX_CONNECTIONS);
        clusterConf.addClient(node1);
        clusterConf.addClient(node2);

        IRiakClient client = RiakFactory.newClient(clusterConf);

        ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        long startTime = System.currentTimeMillis();

        Bucket bucket = client.createBucket("demo_bucket").execute();
        for (int i = 0; i < DATA_COUNT; i++) {
            waitForQueueSizeLessThan(executor, MAX_JOB_QUEUE_CAPACITY);

            executor.execute(newStoringTask(bucket, i));
        }

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        long duration = System.currentTimeMillis() - startTime;

        client.shutdown();

        log("ops per sec: " + DATA_COUNT / (duration / 1000.0));
    }

    private static void waitForQueueSizeLessThan(ThreadPoolExecutor executor, int size)
            throws InterruptedException {

        while (executor.getQueue().size() > size ) { Thread.sleep(10L); }
    }

    private static Runnable newStoringTask(final Bucket bucket, final int counter) {
        return new Runnable() {

            @Override
            public void run() {
                try {
                    if (counter % 1000 == 0) { log("storing key #" + counter); };
                    String key = "demo_key_" + counter;
                    String value = "{'name':'bob','age':18}";
                    bucket.store(key, value)
                            .returnBody(false)
                            .withRetrier(DefaultRetrier.attempts(0))
                            .execute();

                } catch (Exception e) {
                    log(e.toString());
                }
            }
        };
    }

    private static void log(String log) {
        System.out.println(log);
    }

}
