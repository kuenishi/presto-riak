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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class RiakModule
        implements Module
{
    private final String connectorId;

    public RiakModule(String connectorId)
    {
        this.connectorId = checkNotNull(connectorId, "connector id is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(RiakConnector.class).in(Scopes.SINGLETON);
        binder.bind(RiakConnectorId.class).toInstance(new RiakConnectorId(connectorId));
        binder.bind(RiakMetadata.class).in(Scopes.SINGLETON);
        binder.bind(RiakClient.class).in(Scopes.SINGLETON);
        binder.bind(RiakSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(RiakRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(RiakHandleResolver.class).in(Scopes.SINGLETON);
        bindConfig(binder).to(RiakConfig.class);

        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(RiakTable.class));
    }
}
