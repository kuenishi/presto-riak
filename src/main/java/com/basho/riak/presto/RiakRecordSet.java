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

import com.basho.riak.client.IRiakClient;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
//import com.google.common.io.InputSupplier;
//import com.google.common.io.Resources;
import io.airlift.log.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class RiakRecordSet
        implements RecordSet
{
    private final String schemaName;
    private final String tableName;
    private final List<RiakColumnHandle> columnHandles;
    private final List<ColumnType> columnTypes;
    private final List<HostAddress> addresses;
    //private final InputSupplier<InputStream> inputStreamSupplier;

    private static final Logger log = Logger.get(RiakRecordSet.class);


    public RiakRecordSet(RiakSplit split, List<RiakColumnHandle> columnHandles)
    {
        checkNotNull(split, "split is null");
        this.columnHandles = checkNotNull(columnHandles, "column handles is null");
        ImmutableList.Builder<ColumnType> types = ImmutableList.builder();
        for (RiakColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }

        log.debug("new RiakRecordSet");

        this.schemaName = split.getSchemaName();
        this.tableName = split.getTableName();
        this.columnTypes = types.build();
        this.addresses = ImmutableList.copyOf(split.getAddresses());
    }

    @Override
    public List<ColumnType> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new RiakRecordCursor(schemaName, tableName, columnHandles, addresses);
    }
}
