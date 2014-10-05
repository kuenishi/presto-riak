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

import com.basho.riak.client.core.util.BinaryValue;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Strings;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.*;

public class RiakRecordCursor
        implements RecordCursor {
    private static final Logger log = Logger.get(RiakRecordCursor.class);

    //private static final Splitter LINE_SPLITTER = Splitter.on(",").trimResults();

    private final BinaryValue schemaName;
    private final BinaryValue tableName;
    private final List<RiakColumnHandle> columnHandles;
    //private final int[] fieldToColumnIndex;

    //private final Iterator<String> lines;
    private long totalBytes;


    private String[] fields;
    private Map<String, Object> cursor;

    public RiakRecordCursor(String schemaName,
                            String tableName,
                            List<RiakColumnHandle> columnHandles,//, InputSupplier<InputStream> inputStreamSupplier)
                            List<HostAddress> addresses) {
        checkNotNull(schemaName);
        checkState(schemaName.equals("default"));
        checkNotNull(tableName);
        checkNotNull(addresses);
        checkState(!addresses.isEmpty());
        checkState(!columnHandles.isEmpty());

        this.schemaName = BinaryValue.create(schemaName);
        this.tableName = BinaryValue.create(tableName);

        cursor = null;
        fields = new String[columnHandles.size()];

        this.columnHandles = columnHandles;
//        fieldToColumnIndex = new int[columnHandles.size()];
        log.debug(columnHandles.toString());
        for (int i = 0; i < columnHandles.size(); i++) {
            log.debug("%d, %s", i, columnHandles.get(i));
            RiakColumnHandle columnHandle = columnHandles.get(i);
            fields[i] = columnHandle.getColumn().getName();
//            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }
        totalBytes = 0;
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
        return columnHandles.get(field).getColumn().spiType();
    }

    @Override
    public boolean advanceNextPosition() { /*
        if(riakClient == null){
            try{
                riakClient = RiakFactory.newClient(clusterConfig);
                bucket = riakClient.fetchBucket(tableName).execute();
                keyCursor = bucket.keys();
                log.debug("connected to riak");
            }
            catch (RiakException e)
            {
                log.error(e.toString());
                riakClient.shutdown();
                return false;
            }
        }

        if (buffer.isEmpty()) {

            List<String> keys = keyCursor.getAll();

            if(keys.isEmpty() && !keyCursor.hasContinuation())
            {
                log.debug("no more key to fetch");
                //TODO: is this a right place to shut down the connection?
                //riakClient.shutdown();
                return false;
            }
            log.debug("first keys fetched %s", keys);

            for(String key : keys)
            {
                //log.debug("fetching key: %s ", key);
                try{
                    IRiakObject riakObject = bucket.fetch(key).execute();
                    if(riakObject != null)
                    {
                       buffer.add(riakObject);
                    }
                    else
                    {
                        log.error("fetched object was null? %s", riakObject);
                    }
                }
                catch (RiakRetryFailedException e)
                {
                    log.error(e);
                }
            }
            log.info("%d key data fetched", buffer.size());
//            String[] stringKeys = keys.toArray(new String[keys.size()]);
//            List<MultiFetchFuture<IRiakObject>> futures = bucket.multiFetch(stringKeys).execute();
//            for(MultiFetchFuture<IRiakObject> future : futures)
//            {
//                try{
//                    buffer.add(future.get());
//            catch (InterruptedException e)
//            {
//                log.error(e);
//            }
//            catch (ExecutionException e)
//            {
//                log.error(e);
//            }

//            }
        }

        checkState(!buffer.isEmpty());

        IRiakObject riakObject = buffer.remove(0);
        log.debug("first key: %s", riakObject.getKey());
        //String line = lines.next();
        //fields = LINE_SPLITTER.splitToList(line);
        ObjectMapper mapper = new ObjectMapper();
        try {
            cursor = mapper.readValue(riakObject.getValueAsString(), HashMap.class);
            cursor.put("__pkey", riakObject.getKey());
            totalBytes += riakObject.getValueAsString().length();
            return true;
        }catch (IOException e)
        {
            log.debug(e.toString());
        }
*/
        return false;
    }


    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Slice getSlice(int i) {
        return null;
    }

    private String getFieldValue(int field) {
        checkState(fields != null, "Cursor has not been advanced yes");

        //int columnIndex = fieldToColumnIndex[field];
        //return fields[columnIndex];

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
