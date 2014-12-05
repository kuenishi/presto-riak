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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;

import java.util.Arrays;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

// Presto-Riak style table, stored in Riak and also exchanged between presto nodes
public class PRSchema {
    private static final Logger log = Logger.get(PRSchema.class);
    private final Set<String> tables;
    private final Set<String> comments;

    @JsonCreator
    public PRSchema(
            @JsonProperty("tables") Set<String> tables,
            @JsonProperty(value = "comments", required = false) Set<String> comments) {

        this.tables = checkNotNull(tables, "tables is null");
        this.comments = checkNotNull(comments, "columns is null");
    }

    @JsonProperty
    public Set<String> getTables() {
        return tables;
    }

    @JsonProperty
    public Set<String> getComments() {
        return comments;
    }

    public void addTable(PRTable table, String comment) {
        addTable(table.getName());
    }

    private void addTable(String tableName) {
        tables.add(tableName);
    }

    public static PRSchema example() {
        Set<String> ts = Sets.newHashSet();
        Set<String> s = Sets.newHashSet("tse;lkajsdf");
        PRSchema prs = new PRSchema(ts, s);
        prs.addTable("foobartable");
        return prs;
    }
}
