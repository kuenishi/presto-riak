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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

// Presto-Riak style table, stored in Riak and also exchanged between presto nodes
public class PRTable {
    private static final Logger log = Logger.get(PRTable.class);
    private final String name;
    private final List<RiakColumn> columns;
    private final List<ColumnMetadata> columnsMetadata;

    @JsonCreator
    public PRTable(
            @JsonProperty("name") String name,
            @JsonProperty("columns") List<RiakColumn> columns,
            @JsonProperty(value = "comments", required = false) String comments) {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = checkNotNull(name, "name is null");
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        //TODO: For now we assume keys are all in UTF-8
        columnsMetadata.add(new ColumnMetadata(RiakColumnHandle.KEY_COLUMN_NAME, VarcharType.VARCHAR, 0, true, "primary key", true));
        //columnsMetadata.add(new ColumnMetadata(RiakColumnHandle.KEY_COLUMN_NAME, VarbinaryType.VARBINARY, 0, true, "primary key", true));
        columnsMetadata.add(new ColumnMetadata(RiakColumnHandle.VTAG_COLUMN_NAME, VarcharType.VARCHAR, 1, false, "vtag", true));
        int index = 2;

        for (RiakColumn column : this.columns) {
            columnsMetadata.add(new ColumnMetadata(column.getName(), column.getType(), index, column.getIndex(), column.getComment(), false));
            index++;
        }
        this.columnsMetadata = columnsMetadata.build();
    }

    public static Function<PRTable, String> nameGetter() {
        return new Function<PRTable, String>() {
            @Override
            public String apply(PRTable table) {
                return table.getName();
            }
        };
    }

    public static PRTable example(String tableName) {
        List<RiakColumn> cols = Arrays.asList(
                new RiakColumn("col1", VarcharType.VARCHAR, "d1vv", false),
                new RiakColumn("col2", VarcharType.VARCHAR, "d2", true),
                new RiakColumn("poopie", BigintType.BIGINT, "d3", true));
        return new PRTable(tableName, cols, "");

    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public List<RiakColumn> getColumns() {
        return columns;
    }

    public List<ColumnMetadata> getColumnsMetadata() {
        return columnsMetadata;
    }
}
