package com.basho.riak.presto.models;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URI;

/**
 * A node information paired with Riak
 * Created by kuenishi on 14/08/06.
 */
public class PairwiseNode implements Node {

    public final String localNode;
    public final String host;
    public final int port;

    public PairwiseNode(@JsonProperty("localNode") String localNode,
                        @JsonProperty("host") String host,
                        @JsonProperty("port") int port) {
        this.host = host;
        this.port = port;
        this.localNode = localNode;
    }

    @JsonIgnore
    @Override
    public HostAddress getHostAndPort() {
        return HostAddress.fromParts(host, port);
    }

    @JsonProperty
    public String getHost() {
        return host;
    }

    @JsonProperty
    public int getPort() {
        return port;
    }

    @JsonProperty
    public String getLocalNode() {
        return localNode;
    }

    @JsonIgnore
    @Override
    public URI getHttpUri() {
        return URI.create("http://" + host + ":" + port);
    }

    @JsonIgnore
    @Override
    public String getNodeIdentifier() {
        return null;
    }
}
