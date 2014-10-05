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

public class RiakTable {
    private static final Logger log = Logger.get(RiakTable.class);
    private final String name;
    private final List<RiakColumn> columns;
    private final List<ColumnMetadata> columnsMetadata;

    @JsonCreator
    public RiakTable(
            @JsonProperty("name") String name,
            @JsonProperty("columns") List<RiakColumn> columns) {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = checkNotNull(name, "name is null");
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));

        int index = 0;
        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (RiakColumn column : this.columns) {
            columnsMetadata.add(new ColumnMetadata(column.getName(), column.spiType(), index, false));
            index++;
        }
        this.columnsMetadata = columnsMetadata.build();
    }

    public static Function<com.basho.riak.presto.RiakTable, String> nameGetter() {
        return new Function<com.basho.riak.presto.RiakTable, String>() {
            @Override
            public String apply(com.basho.riak.presto.RiakTable table) {
                return table.getName();
            }
        };
    }

    public static RiakTable example(String tableName) {
        List<RiakColumn> cols = Arrays.asList(new RiakColumn("col1", "STRING", false),
                new RiakColumn("col2", "LONG", false));
        return new RiakTable(tableName, cols);

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
