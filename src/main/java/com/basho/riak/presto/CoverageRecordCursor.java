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

import com.basho.riak.client.*;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.query.StreamingOperation;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.ericsson.otp.erlang.*;
import com.facebook.presto.spi.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import io.airlift.log.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import static com.google.common.base.Preconditions.*;

public class CoverageRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(CoverageRecordCursor.class);

    //private static final Splitter LINE_SPLITTER = Splitter.on(",").trimResults();

    private final String schemaName;
    private final String tableName;
    private final List<RiakColumnHandle> columnHandles;
    //private final int[] fieldToColumnIndex;
    private final SplitTask splitTask;
    private final TupleDomain tupleDomain;
    private final DirectConnection directConnection;

    //private final Iterator<String> lines;
    private long totalBytes;

    private Bucket bucket;
    private StreamingOperation<String> keyCursor;

    private List<IRiakObject> buffer;
    private String[] fields;
    private boolean[] has2i;
    private Map<String, Object> cursor;

    public CoverageRecordCursor(String schemaName,
                                String tableName,
                                List<RiakColumnHandle> columnHandles,//, InputSupplier<InputStream> inputStreamSupplier)
                                List<HostAddress> addresses,
                                SplitTask splitTask,
                                TupleDomain tupleDomain,
                                RiakConfig riakConfig,
                                DirectConnection directConnection)
    {
        this.schemaName = checkNotNull(schemaName);
        checkState(schemaName.equals("default"));
        this.tableName = checkNotNull(tableName);
        checkNotNull(addresses);
        checkState(!addresses.isEmpty());
        checkState(!columnHandles.isEmpty());
        this.splitTask = checkNotNull(splitTask);
        this.tupleDomain = checkNotNull(tupleDomain);
        this.directConnection = checkNotNull(directConnection);

        bucket = null;
        keyCursor = null;
        buffer = new Vector<IRiakObject>();
        cursor = null;
        fields = new String[columnHandles.size()];
        has2i = new boolean[columnHandles.size()];

        this.columnHandles = columnHandles;
//        fieldToColumnIndex = new int[columnHandles.size()];

        log.debug(columnHandles.toString());
        log.debug(tupleDomain.toString());

        for (int i = 0; i < columnHandles.size(); i++) {
//            log.debug("%d, %s", i, columnHandles.get(i));
            RiakColumnHandle columnHandle = columnHandles.get(i);
            fields[i] = columnHandle.getColumnName();
            has2i[i] = columnHandle.getIndex();
//            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }
        fetchData();
    }

    private void fetchData()
    {
        totalBytes = 0;

        try{
            DirectConnection conn = directConnection;

            // TODO: if tupleDomain indicates there is a predicate and
            //       the predicate matches to the 2i then fetch via 2i.
            //       if the predicate is on __pkey then also use 2i with <<"key">>.

            OtpErlangList objects = null;

            if(tupleDomain.isAll()) {
                objects = splitTask.fetchAllData(conn, schemaName, tableName);

            }else if(!tupleDomain.isNone()){

                OtpErlangTuple t = buildQuery();
                if(t == null){
                    log.warn("there are no matching index btw %s and %s",
                            columnHandles, tupleDomain);
                    objects = splitTask.fetchAllData(conn, schemaName, tableName);
                }
                else
                {
                    objects = splitTask.fetchViaIndex(conn,
                            schemaName, tableName,
                            buildQuery());
                }
            }
            for(OtpErlangObject o : objects){
                buffer.add(new RiakObject(o));

            }
            log.info("%d key data fetched.", buffer.size());
        }
//        catch (IOException e){
//            log.error(e);
//        }
        catch (OtpErlangExit e){
            log.error(e);
        }
        catch (OtpAuthException e){
            log.error(e);
        }
        catch (OtpErlangDecodeException e){
            log.error(e);
        }
    }

    // {range, Field, Start, End} or {eq, Field, Val} <- columnHandles and tupleDomain
    private OtpErlangTuple buildQuery() //List<RiakColumnHandle> columnHandles,
                                        //TupleDomain tupleDom)
    {

        // case where a='b'
        Map<ColumnHandle, Comparable<?>> fixedValues = tupleDomain.extractFixedValues();
        for(Map.Entry<ColumnHandle, ?> fixedValue : fixedValues.entrySet()){
            RiakColumnHandle c = (RiakColumnHandle)fixedValue.getKey();
            for(RiakColumnHandle columnHandle : columnHandles)
            {
                if(c.getColumnName().equals(columnHandle.getColumnName())
                        && c.getColumnType().equals(columnHandle.getColumnType())
                        && columnHandle.getIndex())
                {
                    String field = null;
                    OtpErlangObject value;
                    if(columnHandle.getColumnType() == ColumnType.LONG)
                    {
                        field = columnHandle.getColumnName() + "_int";
                        Long l = (Long)fixedValue.getValue();
                        value = new OtpErlangLong(l.longValue());
                    }
                    else if(columnHandle.getColumnType() == ColumnType.STRING)
                    {
                        field = columnHandle.getColumnName() + "_bin";
                        String s = (String)fixedValue.getValue();
                        value = new OtpErlangBinary(s.getBytes());
                    }
                    else
                    {
                        continue;
                    }
                    OtpErlangObject[] t = {
                                    new OtpErlangAtom("eq"),
                                    new OtpErlangBinary(field.toCharArray()),
                                    value};

                    return new OtpErlangTuple(t);
                }
            }
        }

        //case where a < b and ... blah
        for(Map.Entry<ColumnHandle, Domain> entry : tupleDomain.getDomains().entrySet())
        {
            RiakColumnHandle c = (RiakColumnHandle)entry.getKey();
            for(RiakColumnHandle columnHandle: columnHandles)
            {
                if(c.getColumnName().equals(columnHandle.getColumnName())
                        && c.getColumnType().equals(columnHandle.getColumnType())
                        && columnHandle.getIndex())
                {
                    String field = null;
                    OtpErlangObject lhs, rhs;
                    Range span = entry.getValue().getRanges().getSpan();
                    if(columnHandle.getColumnType() == ColumnType.LONG)
                    {
                        field = columnHandle.getColumnName() + "_int";
                        Long l = (Long)span.getLow().getValue();
                        Long r = (Long)span.getHigh().getValue();
                        lhs = new OtpErlangLong(l.longValue());
                        rhs = new OtpErlangLong(l.longValue());
                    }
                    else if(columnHandle.getColumnType() == ColumnType.STRING)
                    {
                        field = columnHandle.getColumnName() + "_bin";
                        String l = (String)span.getLow().getValue();
                        String r = (String)span.getHigh().getValue();
                        lhs = new OtpErlangBinary(l.getBytes());
                        rhs = new OtpErlangBinary(r.getBytes());
                    }
                    else
                    {
                        continue;
                    }
                    OtpErlangObject[] t = {
                            new OtpErlangAtom("range"),
                            new OtpErlangBinary(field.toCharArray()),
                            lhs, rhs};

                    return new OtpErlangTuple(t);
                }
            }
        }
        return null;
    }

    @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public ColumnType getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        //log.debug("buffer length> %d", buffer.size());
        if (buffer.isEmpty()) {
            return false;
        }
        //log.debug("buffer length>> %d", buffer.size());

        IRiakObject riakObject = buffer.remove(0);
        //log.debug("first key: %s", riakObject.getKey());
        //String line = lines.next();
        //fields = LINE_SPLITTER.splitToList(line);
        ObjectMapper mapper = new ObjectMapper();
        try {
            //log.debug(riakObject.getKey());
            //log.debug(riakObject.getValueAsString());
            cursor = mapper.readValue(riakObject.getValueAsString(), HashMap.class);
            cursor.put("__pkey", riakObject.getKey());
            totalBytes += riakObject.getValueAsString().length();
            return true;
        }catch (IOException e)
        {
            log.debug(e.toString());
        }

        return false;
    }

    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yes");

        //int columnIndex = fieldToColumnIndex[field];
        //return fields[columnIndex];

        Object o = cursor.get(fields[field]);
        if(o == null){
            return null;
        }
        else
        {
            return o.toString();
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, ColumnType.BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, ColumnType.LONG);
        return Long.parseLong(getFieldValue(field));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, ColumnType.DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public byte[] getString(int field)
    {
        checkFieldType(field, ColumnType.STRING);
        return getFieldValue(field).getBytes(Charsets.UTF_8);
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, ColumnType expected)
    {
        ColumnType actual = getType(field);
        checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }
}
