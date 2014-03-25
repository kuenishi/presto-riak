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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.Split;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RiakRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final String connectorId;
    private static final Logger log = Logger.get(RiakRecordSetProvider.class);

    @Inject
    public RiakRecordSetProvider(RiakConnectorId connectorId)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
    }

    @Override
    public boolean canHandle(Split split)
    {
        // should be able to be casted to RiakSplit
        log.debug("canHandle: %s", split.toString());
        return split instanceof RiakSplit && ((RiakSplit) split).getConnectorId().equals(connectorId);
    }

    @Override
    public RecordSet getRecordSet(Split split, List<? extends ColumnHandle> columns)
    {
        checkNotNull(split, "partitionChunk is null");
        checkArgument(split instanceof RiakSplit);
        log.debug("getRecordSet");

        RiakSplit riakSplit = (RiakSplit) split;
        checkArgument(riakSplit.getConnectorId().equals(connectorId), "split is not for this connector");

        ImmutableList.Builder<RiakColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            checkArgument(handle instanceof RiakColumnHandle);
            handles.add((RiakColumnHandle) handle);
        }

        return new RiakRecordSet(riakSplit, handles.build());
    }
}
