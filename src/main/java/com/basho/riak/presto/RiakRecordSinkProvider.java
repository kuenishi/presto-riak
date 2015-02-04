package com.basho.riak.presto;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.RecordSink;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by kuenishi on 2015/2/4.
 */
public class RiakRecordSinkProvider
        implements ConnectorRecordSinkProvider {

    private final RiakClient client;
    @Inject
    public RiakRecordSinkProvider(RiakClient client)
    {
        this.client = checkNotNull(client);
    }

    @Override
    public RecordSink getRecordSink(ConnectorOutputTableHandle tableHandle) {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof RiakOutputTableHandle, "tableHandle is not an instance of CassandraOutputTableHandle");
        RiakOutputTableHandle handle = (RiakOutputTableHandle) tableHandle;

        return new RiakRecordSink(handle, client);
    }

    @Override
    public RecordSink getRecordSink(ConnectorInsertTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof RiakOutputTableHandle, "tableHandle is not an instance of CassandraOutputTableHandle");
        RiakOutputTableHandle handle = (RiakOutputTableHandle) tableHandle;

        return new RiakRecordSink(handle, client);
    }
}
