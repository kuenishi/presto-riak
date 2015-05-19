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

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import io.airlift.log.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

public final class RiakTableHandle
        implements ConnectorTableHandle {
    private static final Logger log = Logger.get(RiakTableHandle.class);
    private final String connectorId;
    private final String schemaName;
    private final String tableName;

    @JsonCreator
    public RiakTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName) {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.schemaName = checkNotNull(schemaName, "schemaName is null");
        this.tableName = checkNotNull(tableName, "tableName is null");
        log.debug("constuctor...");
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    public SchemaTableName toSchemaTableName() {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(connectorId, schemaName, tableName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        RiakTableHandle other = (RiakTableHandle) obj;
        return Objects.equal(this.connectorId, other.connectorId) &&
                Objects.equal(this.schemaName, other.schemaName) &&
                Objects.equal(this.tableName, other.tableName);
    }

    @Override
    public String toString() {
        log.debug("toString?");

        return Joiner.on(":").join(connectorId, schemaName, tableName);
    }
}
