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

import com.basho.riak.presto.models.CoverageSplit;
import com.basho.riak.presto.models.PRSubTable;
import com.basho.riak.presto.models.RiakColumnHandle;
import com.ericsson.otp.erlang.*;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.type.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.jayway.jsonpath.JsonPath;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.commons.codec.DecoderException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.*;

public class CoverageRecordCursor
        implements RecordCursor {
    private static final Logger log = Logger.get(CoverageRecordCursor.class);

    //private static final Splitter LINE_SPLITTER = Splitter.on(",").trimResults();

    private final CoverageSplit split;
    private final List<RiakColumnHandle> columnHandles;
    private final TupleDomain tupleDomain;

    private final SplitTask splitTask;
    private final DirectConnection directConnection;
    private final List<Map> buffer;
    private final String[] fields;
    private final Slice[] slices;
    private final boolean[] has2i;
    //private final Iterator<String> lines;
    private String pkey;
    private long totalBytes;
    private Map<String, Object> cursor;

    public CoverageRecordCursor(
            CoverageSplit split,
            List<RiakColumnHandle> columnHandles,//, InputSupplier<InputStream> inputStreamSupplier)
            TupleDomain tupleDomain,
            DirectConnection directConnection)
            throws OtpErlangDecodeException, DecoderException {

        this.split = checkNotNull(split);

        //log.debug(columnHandles.toString());
        checkState(!columnHandles.isEmpty(), "Queries just with (*) cannot run anywhere");
        this.splitTask = split.getSplitTask();
        this.tupleDomain = checkNotNull(tupleDomain, "tupleDomain is null");
        this.directConnection = checkNotNull(directConnection);

        buffer = new ArrayList<Map>();
        cursor = null;
        fields = new String[columnHandles.size()];
        slices = new Slice[columnHandles.size()];
        has2i = new boolean[columnHandles.size()];

        this.columnHandles = columnHandles;

        //log.debug(columnHandles.toString());
        log.debug(tupleDomain.toString());

        for (int i = 0; i < columnHandles.size(); i++) {
            RiakColumnHandle columnHandle = columnHandles.get(i);
            fields[i] = columnHandle.getColumn().getName();
            has2i[i] = columnHandle.getColumn().getIndex();
            if (columnHandle.getColumn().getPkey()) {
                pkey = columnHandle.getColumn().getName();
            }
        }
        fetchData();
    }

    private void fetchData() {
        totalBytes = 0;
        String tableName = split.getTableHandle().getTableName();
        try {

            String bucket = PRSubTable.bucketName(tableName);
            log.info("accessing bucket %s for table %s", bucket, tableName);

            DirectConnection conn = directConnection;

            // TODO: if tupleDomain indicates there is a predicate and
            //       the predicate matches to the 2i then fetch via 2i.
            //       if the predicate is on __pkey then also use 2i with <<"key">>.

            OtpErlangList objects = null;

            if (tupleDomain.isAll()) {
                log.info("using coverage query on %s, this may take a long time!!",
                        split.getTableHandle().toString());
                objects = splitTask.fetchAllData(conn,
                        split.getTableHandle().getSchemaName(),
                        bucket);

            } else if (!tupleDomain.isNone()) {

                OtpErlangTuple query = buildQuery();
                log.info("2i query '%s' on %s", query, split.getTableHandle().toString());
                if (query == null) {
                    log.warn("there are no matching index btw %s and %s",
                            columnHandles, tupleDomain);
                    objects = splitTask.fetchAllData(conn,
                            split.getTableHandle().getSchemaName(),
                            bucket);
                } else {

                    objects = splitTask.fetchViaIndex(conn,
                            split.getTableHandle().getSchemaName(), bucket, query);
                }
            }
            for (OtpErlangObject o : objects) {

                InternalRiakObject riakObject = new InternalRiakObject(o);
                totalBytes += riakObject.getValueAsString().length();

                PRSubTable subtable = split.getTable().getSubtable(tableName);
                if (subtable != null) {
                    try {
                        List<Map<String, Object>> jsonRecords = JsonPath.read(riakObject.getValueAsString(),
                                subtable.getPath());
                        for (Map<String, Object> record : jsonRecords) {
                            try {

                                //TODO: utilize hidden column with vtags
                                record.put(RiakColumnHandle.PKEY_COLUMN_NAME, new String(riakObject.getKey(), "UTF-8"));
                                record.put(RiakColumnHandle.VTAG_COLUMN_NAME, riakObject.getVTag());
                                buffer.add(record);
                            } catch (UnsupportedEncodingException e) {
                                log.warn(e.getMessage());
                            }
                        }
                    }catch (IllegalArgumentException e) {
                        log.debug(e.getMessage() + " - JSONPath couldn't parse this string : " + riakObject.getValueAsString());
                    }
                } else {
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        Map record = mapper.readValue(riakObject.getValueAsString(), HashMap.class);

                        //TODO: utilize hidden column with vtags
                        record.put(RiakColumnHandle.PKEY_COLUMN_NAME, new String(riakObject.getKey(), "UTF-8"));
                        record.put(pkey, new String(riakObject.getKey(), "UTF-8"));
                        record.put(RiakColumnHandle.VTAG_COLUMN_NAME, riakObject.getVTag());

                        buffer.add(record);
                    } catch (IOException e) {
                        log.warn(e.getMessage());
                    }
                }

            }
            log.info("%d key data fetched.", buffer.size());
        }

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

    private OtpErlangTuple buildEqQuery(String field, OtpErlangObject value) {
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
                        log.debug(field);
                        Long l = (Long) fixedValue.getValue();
                        return buildIntEqQuery(field, l);
                    } else if (columnHandle.getColumn().getType() == VarcharType.VARCHAR) {
                        field = columnHandle.getColumn().getName() + "_bin";
                        log.debug(field);
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

    private OtpErlangTuple buildIntRangeQuery(String field, Range span) {
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

    private OtpErlangTuple buildBinRangeQuery(String field, Range span) {
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

    private OtpErlangTuple buildRangeQuery(String field, OtpErlangObject from, OtpErlangObject to) {
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
        checkArgument(field >= 0, "Negative field index");
        return columnHandles.get(field).getColumn().getType();
    }

    @Override
    public boolean advanceNextPosition() {
        //log.debug("buffer length> %d", buffer.size());
        if (buffer.isEmpty()) {
            return false;
        }

        Map riakObject = buffer.remove(0);

        cursor = riakObject;
        return true;
    }

    private String getFieldValue(int field) {
        checkState(fields != null, "Cursor has not been advanced yet");

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
        slices[field] = Slices.utf8Slice(getFieldValue(field));
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
