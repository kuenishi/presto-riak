package com.basho.riak.presto;

import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Split;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
public class CoverageSplit implements Split{


    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final String host;
    private final String splitData;

    private final ImmutableList<HostAddress> addresses;

    private static final Logger log = Logger.get(CoverageSplit.class);

//    public CoverageSplit(String connectorId,
//                         String schemaName,
//                         String tableName,
//                         SplitTask splitTask)
//    {
//        this.schemaName = checkNotNull(schemaName, "schema name is null");
//        this.connectorId = checkNotNull(connectorId, "connector id is null");
//        this.tableName = checkNotNull(tableName, "table name is null");
//
//        checkNotNull(splitTask);
//        this.host = splitTask.getHost();
//        this.splitData = splitTask.toString();
//
//        log.debug("%s.%s to %s: ", schemaName, tableName, host, splitData);
//
//        this.addresses = ImmutableList.copyOf(Arrays.asList(HostAddress.fromString(host)));
//        log.debug("%s", addresses);
//    }

    @JsonCreator
    public CoverageSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("host") String host,
            @JsonProperty("splitData") String splitData)
    {
        this.schemaName = checkNotNull(schemaName, "schema name is null");
        this.connectorId = checkNotNull(connectorId, "connector id is null");
        this.tableName = checkNotNull(tableName, "table name is null");
        this.host = checkNotNull(host);
        this.splitData = checkNotNull(splitData);

        log.debug("%s.%s to %s: %s", schemaName, tableName, host, splitData);
        this.addresses = ImmutableList.copyOf(Arrays.asList(HostAddress.fromString(host)));
        //this.addresses = ImmutableList.copyOf(Arrays.asList(HostAddress.fromParts(host, 8080)));

    //    log.debug("%s", addresses);
    }

    public CoverageSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("host") String host)
    {
        this.schemaName = checkNotNull(schemaName, "schema name is null");
        this.connectorId = checkNotNull(connectorId, "connector id is null");
        this.tableName = checkNotNull(tableName, "table name is null");
        this.host = checkNotNull(host);
        this.splitData = "";

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
