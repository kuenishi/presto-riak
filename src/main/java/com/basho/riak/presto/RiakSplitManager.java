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

import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.SplitSource;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class RiakSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final RiakClient riakClient;

    private static final Logger log = Logger.get(RiakSplitManager.class);


    @Inject
    public RiakSplitManager(RiakConnectorId connectorId, RiakClient riakClient)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.riakClient = checkNotNull(riakClient, "client is null");
    }

    @Override
    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof RiakTableHandle && ((RiakTableHandle) tableHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public PartitionResult getPartitions(TableHandle tableHandle, TupleDomain tupleDomain)
    {
        log.debug("splitter");
        checkArgument(tableHandle instanceof RiakTableHandle, "tableHandle is not an instance of RiakTableHandle");
        RiakTableHandle RiakTableHandle = (RiakTableHandle) tableHandle;

        // Riak connector has only one partition
        List<Partition> partitions = ImmutableList.<Partition>of(new RiakPartition(RiakTableHandle.getSchemaName(), RiakTableHandle.getTableName()));
        // Riak connector does not do any additional processing/filtering with the TupleDomain, so just return the whole TupleDomain
        return new PartitionResult(partitions, tupleDomain);
    }

    @Override
    public SplitSource getPartitionSplits(TableHandle tableHandle, List<Partition> partitions)
    {
        checkNotNull(partitions, "partitions is null");
        checkArgument(partitions.size() == 1, "Expected one partition but got %s", partitions.size());
        Partition partition = partitions.get(0);

        checkArgument(partition instanceof RiakPartition, "partition is not an instance of RiakPartition");
        RiakPartition RiakPartition = (RiakPartition) partition;

        RiakTableHandle riakTableHandle = (RiakTableHandle) tableHandle;
        RiakTable table = //RiakTable.example(riakTableHandle.getTableName());
                riakClient.getTable(riakTableHandle.getSchemaName(), riakTableHandle.getTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists", riakTableHandle.getSchemaName(), riakTableHandle.getTableName());

        // add all nodes at the cluster here
        List<Split> splits = Lists.newArrayList();
        String hosts = riakClient.getHosts();

        // TODO: in Riak connector, you only need single access point for each presto worker???
        log.debug(hosts);
        splits.add(new RiakSplit(connectorId, riakTableHandle.getSchemaName(),
                riakTableHandle.getTableName(), hosts));

        Collections.shuffle(splits);
        log.debug("table %s.%s has %d splits.",
                riakTableHandle.getSchemaName(), riakTableHandle.getTableName(),
                splits.size());

        return new FixedSplitSource(connectorId, splits);
    }
}
