package com.basho.riak.presto;

import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import javax.inject.Inject;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Created by kuenishi on 2015/2/4.
 */
public class RiakRecordSink implements RecordSink {

    private static final Logger log = Logger.get(RiakRecordSink.class);

    private RiakClient client;
    private RiakOutputTableHandle handle;
    private int i;
    private HashMap<String, Object> data;
    private String key;
    private String vtag; // TODO: do primer get

    @Inject
    public RiakRecordSink(RiakOutputTableHandle handle, RiakClient client) {
        this.handle = checkNotNull(handle);
        this.client = checkNotNull(client);
        this.i = 0;
        this.data = new HashMap<>();
        this.key = null;
    }

    private void append(Object o) {
        String property = handle.getColumnNames().get(i);
        //Type t = handle.getColumnTypes().get(i);
        if (property.equals(RiakColumnHandle.KEY_COLUMN_NAME)) {
            key = (String) o;
        } else {
            if (property.equals(RiakColumnHandle.VTAG_COLUMN_NAME)) {
                vtag = (String) o;
            } else {
                data.put(property, o);
            }
        }
        i++;
    }

    @Override
    public void appendDouble(double v) {
        append(v);
    }

    @Override
    public void appendLong(long l) {
        append(l);
    }

    @Override
    public void appendString(byte[] bytes) {
        append(new String(bytes));
    }

    @Override
    public void appendNull() {
        append(null);
    }

    @Override
    public void appendBoolean(boolean b) {
        append(b);
    }

    @Override
    public void beginRecord(long l) {
        i = 0;
    }

    @Override
    public void finishRecord() {

        RiakObject obj = new RiakObject().setContentType("application/json");
        // TODO: bind this and don't do new
        ObjectMapper mapper = new ObjectMapper();
        try {
            obj.setValue(BinaryValue.create(mapper.writeValueAsBytes(data)));
            //TODO: set 2i !!!
            //obj.setVTag(vtag);
            SchemaTableName schemaTableName = new SchemaTableName(handle.getSchemaName(), handle.getTableName());
            boolean result = client.insert(schemaTableName, key, obj);
            log.debug("insert result: %s", result);
            i = -1;
        } catch (JsonProcessingException e) {
            log.warn(e.getMessage());
        }
    }

    @Override
    public List<Type> getColumnTypes() {
        return handle.getColumnTypes();
    }

    @Override
    public void rollback() {
        i = 0;
    }

    @Override
    public Collection<Slice> commit() {
        checkState(i == -1, "record not finished");
        // the committer does not need any additional info
        return ImmutableList.of();
    }
}
