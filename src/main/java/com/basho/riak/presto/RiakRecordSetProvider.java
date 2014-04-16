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

import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.Split;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.apache.commons.codec.DecoderException;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RiakRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final String connectorId;
    private final RiakConfig riakConfig;
    private final DirectConnection directConnection;
    private static final Logger log = Logger.get(RiakRecordSetProvider.class);

    @Inject
    public RiakRecordSetProvider(RiakConnectorId connectorId,
                                 RiakConfig riakConfig,
                                 DirectConnection directConnection)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.riakConfig = checkNotNull(riakConfig);
        this.directConnection = checkNotNull(directConnection);

        log.debug(riakConfig.getHost());
        log.debug(riakConfig.getErlangCookie());
        log.debug(riakConfig.getErlangNodeName());
        log.debug(riakConfig.getLocalNode());
    }

    @Override
    public boolean canHandle(Split split)
    {
        // should be able to be casted to RiakSplit
        //log.debug("canHandle: %s", split.toString());
        //checkArgument(split instanceof CoverageSplit);
        if(split instanceof CoverageSplit)
        {
            return ((CoverageSplit) split).getConnectorId().equals(connectorId);
        }
        return false;
    }

    @Override
    public RecordSet getRecordSet(Split split, List<? extends ColumnHandle> columns)
    {
        checkNotNull(split, "partitionChunk is null");
        checkArgument(split instanceof CoverageSplit);
        //log.debug("getRecordSet");

            CoverageSplit coverageSplit = (CoverageSplit)split;
            checkArgument(coverageSplit.getConnectorId().equals(connectorId));

            ImmutableList.Builder<RiakColumnHandle> handles = ImmutableList.builder();
            for (ColumnHandle handle : columns) {
                checkArgument(handle instanceof RiakColumnHandle);
                RiakColumnHandle riakColumnHandle = (RiakColumnHandle)handle;
                boolean has2i = coverageSplit.getIndexedColumns().contains(riakColumnHandle.getColumnName());
                riakColumnHandle.setIndex(has2i);
                handles.add(riakColumnHandle);
            }

            //log.debug("supplying CoverageRecordSet");
        try{
            return new CoverageRecordSet(coverageSplit,
                   handles.build(),
                   riakConfig, coverageSplit.getTupleDomain(),
                   directConnection);
        }
        catch(OtpErlangDecodeException e)
        {
            log.error(e);
        }
        catch(DecoderException e)
        {
            log.error(e);
        }
        return null;
    }
}
