package com.basho.riak.presto.models;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Created by kuenishi on 2015/5/9.
 */
public class PRSubTable {

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
    public static final String SEPARATOR = "/";
    private static final Logger log = Logger.get(PRTable.class);
    private final String name;
    private final String path;
    private final List<RiakColumn> columns;
    private String pkey;

    @JsonCreator
    public PRSubTable(
            @JsonProperty(value = "name", required = true) String name,
            @JsonProperty(value = "columns", required = true) List<RiakColumn> columns,
            @JsonProperty("path") String path) {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = checkNotNull(name, "name is null");
        this.path = checkNotNull(path, "Subtable can't be empty in subtable"); // should check whether valid JSONPath
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));

        int index = 2;

        for (RiakColumn column : this.columns) {
            System.out.println(">" + column.getPkey() + " " + column.getType() + " " + pkey);
            if (column.getPkey() &&
                    column.getType() == VarcharType.VARCHAR &&
                    this.pkey == null) {
                this.pkey = column.getName();
            }
        }
    }

    public void setPkey(String k) {
        this.pkey = k;
    }

    public static Function<PRTable, String> nameGetter() {
        return new Function<PRTable, String>() {
            @Override
            public String apply(PRTable table) {
                return table.getName();
            }
        };
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getPath() {
        return path;
    }

    @JsonProperty
    public List<RiakColumn> getColumns() {
        return columns;
    }

    public List<ColumnMetadata> getColumnsMetadata(String connectorId) {
        Map<String, ColumnHandle> columnHandles = getColumnHandles(connectorId);
        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
        for (ColumnHandle columnHandle : columnHandles.values()) {
            RiakColumnHandle handle = (RiakColumnHandle) columnHandle;
            boolean hidden = false;
            if (handle.getOrdinalPosition() < 2)
                hidden = true;
            builder.add(new ColumnMetadata(handle.getColumn().getName(),
                    handle.getColumn().getType(),
                    handle.getColumn().getPkey(),
                    handle.getColumn().getComment(), hidden));

        }
        return builder.build();
    }

    public Map<String, ColumnHandle> getColumnHandles(String connectorId) {
        //TODO: For now we assume keys are all in UTF-8
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();

        columnHandles.put(RiakColumnHandle.PKEY_COLUMN_NAME,
                new RiakColumnHandle(connectorId,
                        new RiakColumn(RiakColumnHandle.PKEY_COLUMN_NAME,
                                VarcharType.VARCHAR, pkey, true, true),
                        0));
        //columnsMetadata.add(new ColumnMetadata(RiakColumnHandle.PKEY_COLUMN_NAME, VarcharType.VARCHAR, 0, true, pkey, true));
        columnHandles.put(RiakColumnHandle.VTAG_COLUMN_NAME,
                new RiakColumnHandle(connectorId,
                        new RiakColumn(RiakColumnHandle.VTAG_COLUMN_NAME,
                                VarcharType.VARCHAR, "vtag", false, false),
                        1));
        //columnsMetadata.add(new ColumnMetadata(RiakColumnHandle.VTAG_COLUMN_NAME, VarcharType.VARCHAR, 1, false, "vtag", true));
        int index = 2;

        for (RiakColumn column : this.columns) {
            // Column metadata should remember which is primary key, but for now it's
            // impossible, as Presto database has no concept of primary key like this.
            columnHandles.put(column.getName(),
                    new RiakColumnHandle(connectorId, column, index));
            //columnsMetadata.add(new ColumnMetadata(column.getName(), column.getType(),
            //        index, column.getIndex(), column.getComment(), false));
            index++;
        }
        return columnHandles.build();
    }

    public static String parentTableName(String s) {
        String[] parts = s.split(SEPARATOR);
        if (parts.length == 1) {
            return s;
        } else if (parts.length == 2) {
            return parts[0];
        } else {
            throw new IllegalArgumentException();
        }
    }
    public static String bucketName(String fullTableName)
    {
        return parentTableName(fullTableName);
    }

    public boolean match(String fullTableName)
    {
        String[] parts = fullTableName.split(SEPARATOR);
        if (parts.length == 1) {
            throw new IllegalArgumentException();
        } else if (parts.length == 2) {
            return this.name.equals(parts[1]);
        } else {
            throw new IllegalArgumentException();
        }
    }
}
