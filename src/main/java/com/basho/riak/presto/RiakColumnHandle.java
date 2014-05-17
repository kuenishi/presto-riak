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
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import io.airlift.log.Logger;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public final class RiakColumnHandle
        implements ConnectorColumnHandle
{
    public static final String PKEY_COLUMN_NAME = "__pkey";
    private final String connectorId;
    private final RiakColumn column;
    private final int ordinalPosition;

    private static final Logger log = Logger.get(RiakRecordSetProvider.class);

    @JsonCreator
    public RiakColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("column") RiakColumn column,
            @JsonProperty("ordinalPosition") int ordinalPosition)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.column = checkNotNull(column, "column is null");
        this.ordinalPosition = ordinalPosition;
    }

    public RiakColumnHandle(String connectorId, ColumnMetadata columnMetadata)
    {
        this(connectorId,
             new RiakColumn(columnMetadata.getName(),
                     columnMetadata.getType(),
                     // TODO: this default 'false' can be implicit performance lose
                     // TODO: if there be a bug that indexedColumns lost somewhere
                     false),
             columnMetadata.getOrdinalPosition());
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public RiakColumn getColumn()
    {
        return column;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(connectorId, column);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        RiakColumnHandle other = (RiakColumnHandle) obj;
        return Objects.equal(this.connectorId, other.connectorId) &&
                this.column.equals(other.column) &&
                this.ordinalPosition == other.ordinalPosition;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("connectorId", connectorId)
                .add("column", column)
                .add("ordinalPosition", ordinalPosition)
                .toString();
    }
}
