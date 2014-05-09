package com.basho.riak.presto;

import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.internal.cglib.core.$TypeUtils;
import io.airlift.json.JsonCodecFactory;
import io.airlift.log.Logger;
import org.apache.commons.codec.DecoderException;

import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by kuenishi on 14/03/28.
 */
public class CoverageSplit implements ConnectorSplit {


    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final String host;
    private final String splitData;
    private final TupleDomain tupleDomain;
    private final List<String> indexedColumns;


    private final ImmutableList<HostAddress> addresses;

    private static final Logger log = Logger.get(CoverageSplit.class);

    @JsonCreator
    public CoverageSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("host") String host,
            @JsonProperty("splitData") String splitData,
            @JsonProperty("tupleDomain") TupleDomain tupleDomain,
            @JsonProperty("indexedColumns") List<String> indexedColumns)
    {
        this.schemaName = checkNotNull(schemaName, "schema name is null");
        this.connectorId = checkNotNull(connectorId, "connector id is null");
        this.tableName = checkNotNull(tableName, "table name is null");
        this.host = checkNotNull(host);
        this.splitData = checkNotNull(splitData);
        this.tupleDomain = checkNotNull(tupleDomain);
        this.indexedColumns = checkNotNull(indexedColumns);

        //log.debug("%s.%s to %s: %s", schemaName, tableName, host, splitData);
        this.addresses = ImmutableList.copyOf(Arrays.asList(HostAddress.fromString(host)));
        //this.addresses = ImmutableList.copyOf(Arrays.asList(HostAddress.fromParts(host, 8080)));

    }

    public CoverageSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("host") String host,
            @JsonProperty("tupleDomain") TupleDomain tupleDomain,
            @JsonProperty("indexedColumns") List<String> indexedColumns)
    {
        this.schemaName = checkNotNull(schemaName, "schema name is null");
        this.connectorId = checkNotNull(connectorId, "connector id is null");
        this.tableName = checkNotNull(tableName, "table name is null");
        this.host = checkNotNull(host);
        this.splitData = "";
        this.tupleDomain = checkNotNull(tupleDomain);
        this.indexedColumns = checkNotNull(indexedColumns);

        //log.debug("%s.%s to %s: %s", schemaName, tableName, host, splitData);
        this.addresses = ImmutableList.copyOf(Arrays.asList(HostAddress.fromString(host)));
        //this.addresses = ImmutableList.copyOf(Arrays.asList(HostAddress.fromParts(host, 8080)));
    }

    @JsonProperty
    public String getConnectorId(){ return connectorId; }
    @JsonProperty
    public String getSchemaName(){ return schemaName; }
    @JsonProperty
    public String getTableName(){ return tableName; }
    @JsonProperty
    public String getHost(){ return host; }
    @JsonProperty
    public String getSplitData(){ return splitData; }
    @JsonProperty
    public TupleDomain getTupleDomain() { return tupleDomain; }
    @JsonProperty
    public List<String> getIndexedColumns() { return indexedColumns; }

    @Override
    public boolean isRemotelyAccessible() {
        //log.debug(new JsonCodecFactory().jsonCodec(CoverageSplit.class).toJson(this));

        return false;
    }

    @Override
    public Object getInfo() {
        return ImmutableMap.builder()
                .put("connectorId", connectorId)
                .put("host", host)
                .put("splitData", splitData)
                .put("schemaName", schemaName)
                .put("tableName", tableName)
                .put("tupleDomain", tupleDomain)
                .put("indexedColumns", indexedColumns)
                .build();
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        //log.debug("getAddress: %s", addresses);
        //log.debug(new JsonCodecFactory().jsonCodec(CoverageSplit.class).toJson(this));
        return addresses;
    }

    @NotNull
    public SplitTask getSplitTask()
        throws OtpErlangDecodeException, DecoderException
    {
        return new SplitTask(splitData);
    }

}
