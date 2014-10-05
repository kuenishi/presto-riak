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

import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Objects;
import io.airlift.log.Logger;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class RiakPartition
        implements ConnectorPartition {
    private static final Logger log = Logger.get(RiakPartition.class);
    private final String schemaName;
    private final String tableName;
    private final TupleDomain tupleDomain;
    private final List<String> indexedColumns;

    public RiakPartition(String schemaName,
                         String tableName,
                         TupleDomain tupleDomain,
                         List<String> indexedColumns
    ) {
        this.schemaName = checkNotNull(schemaName, "schema name is null");
        this.tableName = checkNotNull(tableName, "table name is null");
        this.tupleDomain = checkNotNull(tupleDomain);
        this.indexedColumns = checkNotNull(indexedColumns);
    }

    @Override
    public String getPartitionId() {
        return schemaName + ":" + tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getIndexedColumns() {
        return indexedColumns;
    }

    @Override
    public TupleDomain getTupleDomain() {
        return tupleDomain;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("tupleDomain", tupleDomain)
                .add("indexColumns", indexedColumns)
                .toString();
    }
}
