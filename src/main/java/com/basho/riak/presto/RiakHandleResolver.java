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
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import io.airlift.log.Logger;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class RiakHandleResolver
        implements ConnectorHandleResolver
{
    private static final Logger log = Logger.get(RiakRecordSetProvider.class);


    private final String connectorId;

    @Inject
    public RiakHandleResolver(RiakConnectorId clientId)
    {
        this.connectorId = checkNotNull(clientId, "clientId is null").toString();
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof RiakTableHandle && ((RiakTableHandle) tableHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return columnHandle instanceof RiakColumnHandle && ((RiakColumnHandle) columnHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public boolean canHandle(Split split)
    {
        if(split instanceof CoverageSplit){
            return ((CoverageSplit) split).getConnectorId().equals(connectorId);
        }
        return false;
    }

    @Override
    public Class<? extends TableHandle> getTableHandleClass()
    {
        return RiakTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return RiakColumnHandle.class;
    }

    @Override
    public Class<? extends Split> getSplitClass()
    {
        return CoverageSplit.class;
    }
}
