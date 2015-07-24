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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

// Presto-Riak style table, stored in Riak and also exchanged between presto nodes
public class PRTable {
    private static final Logger log = Logger.get(PRTable.class);
    private final String name;
    private final List<RiakColumn> columns;
    private final Optional<String> comment;
    private final Optional<List<PRSubTable>> subtables;

    private String pkey;

    @JsonCreator
    public PRTable(
            @JsonProperty(value = "name", required = true) String name,
            @JsonProperty(value = "columns", required = true) List<RiakColumn> columns,
            @JsonProperty(value = "comment", required = false) String comment,
            @JsonProperty(value = "subtables", required = false) List<PRSubTable> subtables) {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = checkNotNull(name, "name is null");
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
        this.comment = Optional.ofNullable(comment);
        this.subtables = Optional.ofNullable(ImmutableList.copyOf(subtables));

        for (RiakColumn column : this.columns) {
            if (column.getPkey() &&
                    column.getType() == VarcharType.VARCHAR &&
                    this.pkey == null) {
                this.pkey = column.getName();
            }
        }
        if (pkey == null) {
            log.warn("Primary Key is not defined in columns effectively. Some queries become slow.");
        }

        // TODO: verify all subtable definitions here
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
    public List<RiakColumn> getColumns() {
        return columns;
    }

    @JsonProperty
    public List<PRSubTable> getSubtables() {
        return subtables.get();
    }

    public List<String> getSubtableNames() {
        List<String> l = new ArrayList<>();
        if (!subtables.isPresent()) {
            return l;
        }
        for (PRSubTable subtable : subtables.get()) {
            l.add(name + PRSubTable.SEPARATOR + subtable.getName());
        }
        return l;
    }

    public PRSubTable getSubtable(String fullTableName) {
        if (fullTableName.equals(name)) {
            return null;
        }
        for (PRSubTable subtable : subtables.get()) {
            //fullTableName is lowercase due to Presto's constraint
            if (subtable.match(fullTableName)) {
                return subtable;
            }
        }
        return null;
    }

    @JsonProperty
    public String getComment() {
        return comment.get();
    }

    public List<ColumnMetadata> getColumnsMetadata(String connectorId) {
        Map<String, ColumnHandle> columnHandles = getColumnHandles(connectorId);
        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
        for (ColumnHandle columnHandle : columnHandles.values()) {
            RiakColumnHandle handle = (RiakColumnHandle) columnHandle;
            boolean hidden = false;
            if (handle.getOrdinalPosition() < 2)
                hidden = true;
            String comment = handle.getColumn().getComment();
            if (handle.getColumn().getPkey()) {
                if (comment == null) {
                    comment = "(key)";
                } else {
                    comment += " (key)";
                }
            }
            builder.add(new ColumnMetadata(handle.getColumn().getName(),
                    handle.getColumn().getType(),
                    handle.getColumn().getPkey(),
                    comment, hidden));

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
            // Column metadata should remember whichi is primary key, but for now it's
            // impossible, as Presto database has no concept of primary key like this.
            columnHandles.put(column.getName(),
                    new RiakColumnHandle(connectorId, column, index));
            //columnsMetadata.add(new ColumnMetadata(column.getName(), column.getType(),
            //        index, column.getIndex(), column.getComment(), false));
            index++;
        }
        return columnHandles.build();
    }

    public String toString() {
        String ret = name + ":{";
        for (RiakColumn col : columns) {
            ret += col.toString();
            ret += " ";
        }
        ret += "} ";
        for (PRSubTable subtable : subtables.get()) {
            ret += subtable.toString();
        }
        return ret;
    }

}