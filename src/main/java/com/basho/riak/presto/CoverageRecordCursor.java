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

import com.ericsson.otp.erlang.*;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.type.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import static com.google.common.base.Preconditions.*;

public class CoverageRecordCursor
        implements RecordCursor {
    private static final Logger log = Logger.get(CoverageRecordCursor.class);

    //private static final Splitter LINE_SPLITTER = Splitter.on(",").trimResults();

    private final String schemaName;
    private final String tableName;
    private final List<RiakColumnHandle> columnHandles;
    //private final int[] fieldToColumnIndex;
    private final SplitTask splitTask;
    private final TupleDomain tupleDomain;
    private final DirectConnection directConnection;
    private final List<InternalRiakObject> buffer;
    private final String[] fields;
    private final Slice[] slices;
    private final boolean[] has2i;
    //private final Iterator<String> lines;
    private String pkey;
    private long totalBytes;
    private Map<String, Object> cursor;

    public CoverageRecordCursor(String schemaName,
                                String tableName,
                                List<RiakColumnHandle> columnHandles,//, InputSupplier<InputStream> inputStreamSupplier)
                                List<HostAddress> addresses,
                                SplitTask splitTask,
                                TupleDomain tupleDomain,
                                RiakConfig riakConfig,
                                DirectConnection directConnection) {
        this.schemaName = checkNotNull(schemaName);
        this.tableName = checkNotNull(tableName);
        checkNotNull(addresses);
        checkState(!addresses.isEmpty());
        // TODO: if (*) selected, columnHandles gets really empty...
        log.debug(columnHandles.toString());
        checkState(!columnHandles.isEmpty(), "Queries just with (*) cannot run anywhere");
        this.splitTask = checkNotNull(splitTask, "splitTask is null");
        this.tupleDomain = checkNotNull(tupleDomain, "tupleDomain is null");
        this.directConnection = checkNotNull(directConnection);

        buffer = new Vector<InternalRiakObject>();
        cursor = null;
        fields = new String[columnHandles.size()];
        slices = new Slice[columnHandles.size()];
        has2i = new boolean[columnHandles.size()];

        this.columnHandles = columnHandles;
//        fieldToColumnIndex = new int[columnHandles.size()];

        log.debug(columnHandles.toString());
        log.debug(tupleDomain.toString());

        for (int i = 0; i < columnHandles.size(); i++) {
//            log.debug("%d, %s", i, columnHandles.get(i));
            RiakColumnHandle columnHandle = columnHandles.get(i);
            fields[i] = columnHandle.getColumn().getName();
            has2i[i] = columnHandle.getColumn().getIndex();
//            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
            if (columnHandle.getColumn().getPkey()){
                pkey = columnHandle.getColumn().getName();
            }
        }
        fetchData();
    }

    private void fetchData() {
        totalBytes = 0;

        try {
            DirectConnection conn = directConnection;

            // TODO: if tupleDomain indicates there is a predicate and
            //       the predicate matches to the 2i then fetch via 2i.
            //       if the predicate is on __pkey then also use 2i with <<"key">>.

            OtpErlangList objects = null;

            if (tupleDomain.isAll()) {
                log.info("using coverage query on %s:%s, this may take a long time!!",
                        schemaName, tableName);
                objects = splitTask.fetchAllData(conn, schemaName, tableName);

            } else if (!tupleDomain.isNone()) {

                OtpErlangTuple query = buildQuery();
                log.info("2i query '%s' on %s:%s", query, schemaName, tableName);
                if (query == null) {
                    log.warn("there are no matching index btw %s and %s",
                            columnHandles, tupleDomain);
                    objects = splitTask.fetchAllData(conn, schemaName, tableName);
                } else {

                    objects = splitTask.fetchViaIndex(conn,
                            schemaName, tableName, query);
                }
            }
            for (OtpErlangObject o : objects) {
                buffer.add(new InternalRiakObject(o));

            }
            log.info("%d key data fetched.", buffer.size());
        }
//        catch (IOException e){
//            log.error(e);
//        }
        catch (OtpErlangExit e) {
            log.error(e);
        } catch (OtpAuthException e) {
            log.error(e);
        } catch (OtpErlangDecodeException e) {
            log.error(e);
        }
    }

    @Override
    public long getReadTimeNanos() {
        log.debug("getReadTimeNanos");
        return 0;
    }

    private OtpErlangTuple buildEqQuery(String field, OtpErlangObject value){
        OtpErlangObject[] t = {
                new OtpErlangAtom("eq"),
                new OtpErlangBinary(field.getBytes()),
                value};
        return new OtpErlangTuple(t);
    }
    private OtpErlangTuple buildBinEqQuery(String field, Slice s) {
        return buildEqQuery(field, new OtpErlangBinary(s.getBytes()));
    }
    private OtpErlangTuple buildIntEqQuery(String field, Long l) {
        return buildEqQuery(field, new OtpErlangLong(l));
    }
    // {range, Field, Start, End} or {eq, Field, Val} <- columnHandles and tupleDomain
    private OtpErlangTuple buildQuery() //List<RiakColumnHandle> columnHandles,
    {
        // case where a='b'
        Map<ColumnHandle, Comparable<?>> fixedValues = tupleDomain.extractFixedValues();
        for (Map.Entry<ColumnHandle, Comparable<?>> fixedValue : fixedValues.entrySet()) {
            log.debug("> %s (%s)", fixedValue, fixedValue.getClass());
            log.debug(">> %s", fixedValue.getKey());

            checkNotNull(fixedValue.getKey());
            checkArgument(fixedValue.getKey() instanceof ColumnHandle);
            checkArgument(fixedValue.getKey() instanceof RiakColumnHandle);

            RiakColumnHandle c = (RiakColumnHandle) fixedValue.getKey();
            if (c.getColumn().getPkey()) {
                Slice s = (Slice) fixedValue.getValue();
                return buildBinEqQuery("$key", s);
            }

            for (RiakColumnHandle columnHandle : columnHandles) {
                if (c.matchAndBothHasIndex(columnHandle)) {
                    String field = null;

                    if (columnHandle.getColumn().getType() == BigintType.BIGINT) {
                        field = columnHandle.getColumn().getName() + "_int";
                        Long l = (Long) fixedValue.getValue();
                        return buildIntEqQuery(field, l);
                    } else if (columnHandle.getColumn().getType() == VarcharType.VARCHAR) {
                        field = columnHandle.getColumn().getName() + "_bin";
                        Slice s = (Slice) fixedValue.getValue();
                        return buildBinEqQuery(field, s);
                    }
                }
            }
        }

        //case where a < b and ... blah
        Map<RiakColumnHandle, Domain> map = tupleDomain.getDomains();
        for (Map.Entry<RiakColumnHandle, Domain> entry : map.entrySet()) {
            RiakColumnHandle c = entry.getKey();
            Range span = entry.getValue().getRanges().getSpan();

            if (c.getColumn().getPkey()) {
                return buildBinRangeQuery("$key", span);
            }

            for (RiakColumnHandle columnHandle : columnHandles) {
                if (c.matchAndBothHasIndex(columnHandle)) {
                    String field = null;
                    //log.debug("value:%s, range:%s, span:%s",
                    //        entry.getValue(), entry.getValue().getRanges(),span);
                    //log.debug("min: %s max:%s", span.getLow(), span.getHigh());
                    if (columnHandle.getColumn().getType() == BigintType.BIGINT) {
                        field = columnHandle.getColumn().getName() + "_int";
                        return buildIntRangeQuery(field, span);

                    } else if (columnHandle.getColumn().getType() == VarcharType.VARCHAR) {
                        field = columnHandle.getColumn().getName() + "_bin";
                        return buildBinRangeQuery(field, span);
                    }
                }
            }
        }
        return null;
    }

    private OtpErlangTuple buildIntRangeQuery(String field, Range span)
    {
        // NOTE: Both Erlang and JSON can express smaller integer than Long.MIN_VALUE
        Long l = Long.MIN_VALUE;
        if (!span.getLow().isLowerUnbounded()) {
            l = (Long) span.getLow().getValue();
        }
        // NOTE: Both Erlang and JSON can express greater integer lang Long.MAX_VALUE
        Long r = Long.MAX_VALUE;
        if (!span.getHigh().isUpperUnbounded()) {
            r = (Long) span.getHigh().getValue();
        }
        return buildRangeQuery(field, new OtpErlangLong(l), new OtpErlangLong(r));
    }
    private OtpErlangTuple buildBinRangeQuery(String field, Range span)
    {
        byte[] from = {0};
        if (!span.getLow().isLowerUnbounded()) {
            from = ((String) span.getLow().getValue()).getBytes();
        }
        Byte m2 = Byte.MAX_VALUE;
        byte[] to = {m2};
        if (!span.getHigh().isUpperUnbounded()) {
            to = ((String) span.getHigh().getValue()).getBytes();
        }
        return buildRangeQuery(field, new OtpErlangBinary(from), new OtpErlangBinary(to));
    }
    private OtpErlangTuple buildRangeQuery(String field, OtpErlangObject from, OtpErlangObject to)
    {
        OtpErlangObject[] t = {
                new OtpErlangAtom("range"),
                new OtpErlangBinary(field.getBytes()),
                from, to};
        return new OtpErlangTuple(t);
    }

    @Override
    public long getTotalBytes() {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes() {
        return totalBytes;
    }

    @Override
    public Type getType(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumn().getType();
    }

    @Override
    public boolean advanceNextPosition() {
        //log.debug("buffer length> %d", buffer.size());
        if (buffer.isEmpty()) {
            return false;
        }
        //log.debug("buffer length>> %d", buffer.size());

        InternalRiakObject riakObject = buffer.remove(0);

        ObjectMapper mapper = new ObjectMapper();
        try {
            //log.debug("riakObject.getKey() => %s", new String(riakObject.getKey(), "UTF-8"));
            //log.debug("riakObject.getValue() => %s", riakObject.getValueAsString());
            cursor = mapper.readValue(riakObject.getValueAsString(), HashMap.class);

            //TODO: utilize hidden column with vtags
            String pkeyValue = new String(riakObject.getKey(), "UTF-8");
            cursor.put(RiakColumnHandle.PKEY_COLUMN_NAME, pkeyValue);
            cursor.put(RiakColumnHandle.VTAG_COLUMN_NAME, riakObject.getVTag());
            if (pkey != null) {
                cursor.put(pkey, pkeyValue);
            }
            totalBytes += riakObject.getValueAsString().length();
            return true;
        } catch (IOException e) {
            log.debug(e.toString());
        }

        return false;
    }

    private String getFieldValue(int field) {
        checkState(fields != null, "Cursor has not been advanced yet");

        //int columnIndex = fieldToColumnIndex[field];
        //return fields[columnIndex];
        //log.debug("field #%d, %s => %s, %s", field, fields[field], cursor.get(fields[field]), cursor);
        Object o = cursor.get(fields[field]);
        if (o == null) {
            return null;
        } else {
            return o.toString();
        }
    }

    @Override
    public boolean getBoolean(int field) {
        checkFieldType(field, BooleanType.BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field) {
        checkFieldType(field, BigintType.BIGINT);
        return Long.parseLong(getFieldValue(field));
    }

    @Override
    public double getDouble(int field) {
        checkFieldType(field, DoubleType.DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field) {
        //return slices[i];
        if (slices[field] == null) {
            slices[field] = Slices.utf8Slice(getFieldValue(field));
        } else {
            slices[field].setBytes(0, getFieldValue(field).getBytes());
        }
        //log.debug("getSlice called: %s", slices[field]);

        return slices[field];
    }


    @Override
    public boolean isNull(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, Type expected) {
        Type actual = getType(field);
        checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close() {
    }
}
