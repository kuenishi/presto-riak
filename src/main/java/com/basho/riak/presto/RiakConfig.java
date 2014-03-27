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

import java.net.URI;

public class RiakConfig
{
    private String hosts;
    private String localNode;
    private String erlangNodeName = null; // name for distributed erlang like 'presto@127.0.0.1'
    private String erlangCookie = null;

    public static String ERLANG_NODE_NAME = "presto@127.0.0.1";
    public static String ERLANG_COOKIE = "riak";

    @Config("riak.hosts")
    public RiakConfig setHosts(String hosts)
    {
        this.hosts = hosts;
        return this;
    }

    @Config("erlang.riak.node") // like 'riak@127.0.0.1'
    public RiakConfig setLocalNode(String node)
    {
        this.localNode = node;
        return this;
    }
    @Config("erlang.presto.node") // like 'presto@127.0.0.1'
    public RiakConfig setPrestoName(String erlangNodeName)
    {
        this.erlangNodeName = erlangNodeName;
        return this;
    }
    @Config("erlang.cookie")
    public RiakConfig setErlangCookie(String cookie)
    {
        this.erlangCookie = cookie;
        return this;
    }

    @NotNull
    public String getHosts()
    {
        return hosts;
    }

    public String getLocalNode() { return localNode; }

    public String getErlangNodeName()
    {
        if(erlangNodeName == null)
        {
            return ERLANG_NODE_NAME;
        }
        else
        {
            return this.erlangNodeName;
        }
    }

    public String getErlangCookie()
    {
        if(erlangCookie == null)
        {
            return ERLANG_COOKIE;
        }
        else
        {
            return erlangCookie;
        }
    }
}
