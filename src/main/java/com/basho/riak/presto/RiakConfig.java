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

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class RiakConfig {
    public static String ERLANG_NODE_NAME = "presto@127.0.0.1";
    public static String ERLANG_COOKIE = "riak";
    private String host = "localhost";
    private int port = 8087;
    private int prestoPort = 8080;
    private String localNode = "127.0.0.1";
    private String erlangNodeName = null; // name for distributed erlang like 'presto@127.0.0.1'
    private String erlangCookie = null;

    @NotNull
    public String getHost() {
        return host;
    }

    @Config("riak.pb.host")
    public RiakConfig setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public int getPrestoPort() {
        return prestoPort;
    }

    @Config("http-server.http.port")
    public RiakConfig setPrestoPort(int port) {
        this.prestoPort = port;
        return this;
    }

    public String getLocalNode() {
        return localNode;
    }

    @Config("riak.erlang.node") // like 'riak@127.0.0.1'
    public RiakConfig setLocalNode(String node) {
        this.localNode = node;
        return this;
    }

    public String getErlangNodeName() {
        if (erlangNodeName == null) {
            return ERLANG_NODE_NAME;
        } else {
            return this.erlangNodeName;
        }
    }

    @Config("presto.erlang.node") // like 'presto@127.0.0.1'
    public RiakConfig setErlangNodeName(String erlangNodeName) {
        this.erlangNodeName = erlangNodeName;
        return this;
    }

    public String getErlangCookie() {
        if (erlangCookie == null) {
            return ERLANG_COOKIE;
        } else {
            return erlangCookie;
        }
    }

    @Config("presto.erlang.cookie")
    public RiakConfig setErlangCookie(String cookie) {
        this.erlangCookie = cookie;
        return this;
    }
}
