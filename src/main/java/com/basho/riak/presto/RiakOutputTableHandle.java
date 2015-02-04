package com.basho.riak.presto;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by kuenishi on 2015/2/4.
 */
public class RiakOutputTableHandle
        implements ConnectorOutputTableHandle, ConnectorInsertTableHandle {

    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final List<String> columnNames;
    private final List<Type> columnTypes;

    @JsonCreator
    public RiakOutputTableHandle(@JsonProperty("connectorId") String connectorId,
                                 @JsonProperty("schemaName") String schemaName,
                                 @JsonProperty("tableName") String tableName,
                                 @JsonProperty("columnNames") List<String> columnNames,
                                 @JsonProperty("columnTypes") List<Type> columnTypes) {
        this.connectorId = checkNotNull(connectorId, "clientId is null");
        this.schemaName = checkNotNull(schemaName, "schemaName is null");
        this.tableName = checkNotNull(tableName, "tableName is null");

        checkNotNull(columnNames, "columnNames is null");
        checkNotNull(columnTypes, "columnTypes is null");
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes sizes don't match");
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.columnTypes = ImmutableList.copyOf(columnTypes);
    }

    @JsonProperty
    public List<String> getColumnNames() {
        return columnNames;
    }

    @JsonProperty
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }
}
