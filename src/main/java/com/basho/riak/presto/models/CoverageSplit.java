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

import com.basho.riak.presto.SplitTask;
import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.apache.commons.codec.DecoderException;

import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class CoverageSplit implements ConnectorSplit {

    private static final Logger log = Logger.get(CoverageSplit.class);

    private final RiakTableHandle tableHandle;
    private final PRTable table;
    private final String host;
    private final String splitData;
    private final TupleDomain<ColumnHandle> tupleDomain;

    @JsonCreator
    public CoverageSplit(
            @JsonProperty("tableHandle") RiakTableHandle tableHandle,
            @JsonProperty("table") PRTable table,
            @JsonProperty("host") String host,
            @JsonProperty("splitData") String splitData,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain) {

        this.tableHandle = checkNotNull(tableHandle);
        this.table = checkNotNull(table);
        this.host = checkNotNull(host);
        this.splitData = checkNotNull(splitData);
        this.tupleDomain = checkNotNull(tupleDomain);
    }

    @JsonProperty
    public RiakTableHandle getTableHandle() {
        return tableHandle;
    }

    @JsonProperty
    public PRTable getTable() {
        return table;
    }

    @JsonProperty
    public String getHost() {
        return host;
    }

    @JsonProperty
    public String getSplitData() {
        return splitData;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain() {
        return tupleDomain;
    }

    @Override
    public boolean isRemotelyAccessible() {
        //log.debug(new JsonCodecFactory().jsonCodec(CoverageSplit.class).toJson(this));

        return false;
    }

    @Override
    public Object getInfo() {
        return ImmutableMap.builder()
                .put("tableHandle", tableHandle)
                .put("table", table)
                .put("host", host)
                .put("splitData", splitData)
                .put("tupleDomain", tupleDomain)
                .build();
    }

    @Override
    public List<HostAddress> getAddresses() {
        //log.debug("getAddress: %s", addresses);
        //log.debug(new JsonCodecFactory().jsonCodec(CoverageSplit.class).toJson(this));
        return ImmutableList.copyOf(Arrays.asList(HostAddress.fromString(host)));
    }

    @NotNull
    public SplitTask getSplitTask()
            throws OtpErlangDecodeException, DecoderException {
        return new SplitTask(splitData);
    }

}
