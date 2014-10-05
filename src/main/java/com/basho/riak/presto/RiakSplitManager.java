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

import com.facebook.presto.spi.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;

import javax.inject.Inject;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.*;

public class RiakSplitManager
        implements ConnectorSplitManager {
    private static final Logger log = Logger.get(RiakSplitManager.class);
    private final String connectorId;
    private final RiakClient riakClient;
    private final RiakConfig riakConfig;
    private final DirectConnection directConnection;


    @Inject
    public RiakSplitManager(RiakConnectorId connectorId, RiakClient riakClient,
                            RiakConfig config, DirectConnection directConnection) {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.riakClient = checkNotNull(riakClient, "client is null");
        this.riakConfig = checkNotNull(config);
        this.directConnection = checkNotNull(directConnection);
    }


    // TODO: get the right partitions right here
    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain tupleDomain) {
        checkArgument(tableHandle instanceof RiakTableHandle, "tableHandle is not an instance of RiakTableHandle");
        RiakTableHandle riakTableHandle = (RiakTableHandle) tableHandle;

        log.info("==========================tupleDomain=============================");
        log.info(tupleDomain.toString());

        RiakTable table = null;//RiakTable.example(riakTableHandle.getTableName());
        try {
            table = riakClient.getTable(riakTableHandle.getSchemaName(), riakTableHandle.getTableName());

        } catch (Exception e) {
            log.error("interrupted: %s", e.toString());
        }

        List<String> indexedColumns = new LinkedList<String>();
        for (RiakColumn riakColumn : table.getColumns()) {
            if (riakColumn.getIndex()) {
                indexedColumns.add(riakColumn.getName());
            }
        }

        // Riak connector has only one partition
        List<ConnectorPartition> partitions = ImmutableList.<ConnectorPartition>of(new RiakPartition(riakTableHandle.getSchemaName(),
                riakTableHandle.getTableName(),
                tupleDomain,
                indexedColumns));

        // Riak connector does not do any additional processing/filtering with the TupleDomain, so just return the whole TupleDomain
        return new ConnectorPartitionResult(partitions, tupleDomain);
    }

    // TODO: return correct splits from partitions
    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle tableHandle,
                                                   List<ConnectorPartition> partitions) {
        checkNotNull(partitions, "partitions is null");
        checkArgument(partitions.size() == 1, "Expected one partition but got %s", partitions.size());
        ConnectorPartition partition = partitions.get(0);

        checkArgument(partition instanceof RiakPartition, "partition is not an instance of RiakPartition");
        RiakPartition riakPartition = (RiakPartition) partition;

        RiakTableHandle riakTableHandle = (RiakTableHandle) tableHandle;
        RiakTable table = null; //RiakTable.example(riakTableHandle.getTableName());
        try {
            table = riakClient.getTable(riakTableHandle.getSchemaName(), riakTableHandle.getTableName());
        } catch (Exception e) {
        }
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists", riakTableHandle.getSchemaName(), riakTableHandle.getTableName());

        log.debug("> %s", table.getColumns().toString());
        // add all nodes at the cluster here
        List<ConnectorSplit> splits = Lists.newArrayList();
        String hosts = riakClient.getHosts();
        log.debug(hosts);

        if (riakConfig.getLocalNode() != null) {
            // TODO: make coverageSplits here

            //try {
            DirectConnection conn = directConnection;
            //conn.connect(riak);
            //conn.ping();
            Coverage coverage = new Coverage(conn);
            coverage.plan();
            List<SplitTask> splitTasks = coverage.getSplits();

            log.debug("print coverage plan==============");
            log.debug(coverage.toString());

            for (SplitTask split : splitTasks) {
                log.debug("============printing split data at " + split.getHost() + "===============");
                //log.debug(((OtpErlangObject)split.getTask()).toString());
                //log.debug(split.toString());

                //split.fetchAllData(conn, "default", "foobartable");

                splits.add(new CoverageSplit(connectorId,
                        riakTableHandle.getSchemaName(),
                        riakTableHandle.getTableName(),
                        split.getHost(),
                        split.toString(),
                        partition.getTupleDomain(),
                        ((RiakPartition) partition).getIndexedColumns()));
            }
            //}
//            catch (java.io.IOException e){
//                log.error(e);
//            }
//            catch (OtpAuthException e){
//                log.error(e);
//            }
//            catch (OtpErlangExit e)
//            {
//                log.error(e);
//            }
        } else {
            // TODO: in Riak connector, you only need single access point for each presto worker???
            log.debug(hosts);
            splits.add(new CoverageSplit(connectorId, riakTableHandle.getSchemaName(),
                    riakTableHandle.getTableName(), hosts,
                    partition.getTupleDomain(),
                    ((RiakPartition) partition).getIndexedColumns()));

        }
        log.debug("table %s.%s has %d splits.",
                riakTableHandle.getSchemaName(), riakTableHandle.getTableName(),
                splits.size());

        Collections.shuffle(splits);
        return new FixedSplitSource(connectorId, splits);
    }
}
