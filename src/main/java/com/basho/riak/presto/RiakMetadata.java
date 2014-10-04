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

import com.facebook.presto.spi.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RiakMetadata
        extends ReadOnlyConnectorMetadata
{
    private final String connectorId;

    private static final Logger log = Logger.get(RiakMetadata.class);

    private final RiakClient riakClient;

    @Inject
    public RiakMetadata(RiakConnectorId connectorId, RiakClient riakClient)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.riakClient = checkNotNull(riakClient, "client is null");
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession connectorSession, SchemaTableName schemaTableName)
    {
        log.info("getTableHandle;");
        if (!listSchemaNames(connectorSession).contains(schemaTableName.getSchemaName())) {
            log.error("no schema %d found", schemaTableName);
            return null;
        }

        RiakTable table = null;
        try {
            table = riakClient.getTable(schemaTableName.getSchemaName(),
                    schemaTableName.getTableName());
        }catch (Exception e) {
        }
        if (table == null) {
            log.error("no tables found at %d", schemaTableName);
            return null;
        }
        return new RiakTableHandle(connectorId, schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }

    // called from `show schemas`;
    @Override
    public List<String> listSchemaNames(ConnectorSession connectorSession) {
        // TODO: support other schema name than default!!
        log.info("RiakMetadata.listSchemaNames();");
        log.info("%s", connectorSession);
        List<String> list = Arrays.asList("default");
        return ImmutableList.copyOf(riakClient.getSchemaNames());
    }


    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle table)
    {
        log.info("getTableMetadata");
        checkArgument(table instanceof RiakTableHandle, "tableHandle is not an instance of RiakTableHandle");
        RiakTableHandle RiakTableHandle = (RiakTableHandle) table;
        checkArgument(RiakTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName tableName = new SchemaTableName(RiakTableHandle.getSchemaName(), RiakTableHandle.getTableName());

        return getTableMetadata(tableName);
    }

    // called from `show tables;`
    @Override
    public List<SchemaTableName> listTables(ConnectorSession connectorSession, String schemaNameOrNull)
    {

        log.info("listTables for %s;", schemaNameOrNull);

        Set<String> schemaNames;
        if (schemaNameOrNull != null) {
            schemaNames = ImmutableSet.of(schemaNameOrNull);
        }
        else {
            schemaNames = riakClient.getSchemaNames();
            log.info("%s schemas.", schemaNames);
            log.info(schemaNames.toString());
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        try {
            for (String schemaName : schemaNames) {
                log.info(">>> schemaName=%s", schemaName);
                for (String tableName : riakClient.getTableNames(schemaName)) {
                    log.info("table %s found.", tableName);
                    builder.add(new SchemaTableName(schemaName, tableName));
                }
            }
        }catch(Exception e){}

        log.info("listTables for %s: %d tables found", schemaNameOrNull,
                schemaNames.size());

        return builder.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession connectorSession,
                                                                       SchemaTablePrefix prefix)
    {
        log.info("listTableColumns");
        checkNotNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(connectorSession, prefix.getSchemaName())) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

  /*  @Override
    public ConnectorColumnHandle getColumnHandle(ConnectorTableHandle tableHandle, String columnName)
    {
        log.info("getColumnHandle");
        return getColumnHandles(tableHandle).get(columnName);
    }
    */

    @Override
    public ConnectorColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        log.debug("getSampleWeightColumnHandle;");
        return null;
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof RiakTableHandle, "tableHandle is not an instance of RiakTableHandle");
        RiakTableHandle riakTableHandle = (RiakTableHandle) tableHandle;
        checkArgument(riakTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

//        RiakTable table = RiakClient.getTable(RiakTableHandle.getSchemaName(), RiakTableHandle.getTableName());
        RiakTable table = null;
        try {
            table = riakClient.getTable(riakTableHandle.getSchemaName(), riakTableHandle.getTableName());
        }catch (Exception e){}

        if (table == null) {
            throw new TableNotFoundException(riakTableHandle.toSchemaTableName());
        }
        log.debug("table %s found.", riakTableHandle.getTableName());

        ImmutableMap.Builder<String, ConnectorColumnHandle> columnHandles = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : table.getColumnsMetadata()) {
            columnHandles.put(columnMetadata.getName(), new RiakColumnHandle(connectorId, columnMetadata));
        }
        return columnHandles.build();
    }


    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        log.info("getTableMetadata>> %s", tableName.toString());
//        if (!listSchemaNames().contains(tableName.getSchemaName())) {
 //           return null;
  //      }

        RiakTable table = null; //RiakTable.example(tableName.getTableName());
        try {
            table = riakClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        }catch (Exception e){

        }
        if (table == null) {
            throw new TableNotFoundException(tableName);
        }
        log.debug("table %s found.", tableName.getTableName());
        log.debug("%s", table.toString());

        List<ColumnMetadata> l = table.getColumnsMetadata();
        log.debug("table %s with %d columns.", tableName.getTableName(), l.size());

        return new ConnectorTableMetadata(tableName,l);
    }

  /*  private List<SchemaTableName> listTables(SchemaTablePrefix prefix)
    {
        log.debug("listTalbles for %s", prefix.getSchemaName());
        if (prefix.getSchemaName() == null) {
          //  return listTables(prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }
*/
    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ConnectorColumnHandle columnHandle)
    {
        log.info("getColumnMetadata");
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");
        checkArgument(tableHandle instanceof RiakTableHandle, "tableHandle is not an instance of RiakTableHandle");
        checkArgument(((RiakTableHandle) tableHandle).getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        checkArgument(columnHandle instanceof RiakColumnHandle, "columnHandle is not an instance of RiakColumnHandle");
        RiakColumnHandle h = (RiakColumnHandle)columnHandle;
        //return ((RiakColumnHandle) columnHandle).getColumnMetadata();
        return new ColumnMetadata(h.getColumn().getName(), h.getColumn().spiType(), h.getOrdinalPosition(), false);
    }
}
