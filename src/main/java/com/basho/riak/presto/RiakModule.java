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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.json.ObjectMapperProvider;

import javax.inject.Inject;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class RiakModule
        implements Module {
    private final String connectorId;
    private final TypeManager typeManager;

    @Inject
    public RiakModule(String connectorId, TypeManager typeManager) {
        this.connectorId = checkNotNull(connectorId, "connector id is null");
        this.typeManager = checkNotNull(typeManager, "type manager is null");
    }

    @Override
    public void configure(Binder binder) {

        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(RiakConnector.class).in(Scopes.SINGLETON);
        binder.bind(RiakConnectorId.class).toInstance(new RiakConnectorId(connectorId));
        binder.bind(RiakMetadata.class).in(Scopes.SINGLETON);
        //binder.bind(RiakClient.class).in(Scopes.SINGLETON);
        binder.bind(RiakClient.class).in(Scopes.NO_SCOPE);

        binder.bind(DirectConnection.class).in(Scopes.SINGLETON);
        binder.bind(RiakSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(DirectConnection.class).in(Scopes.SINGLETON);
        binder.bind(RiakRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(RiakHandleResolver.class).in(Scopes.SINGLETON);
        bindConfig(binder).to(RiakConfig.class);

        binder.bind(ObjectMapper.class).toProvider(ObjectMapperProvider.class);
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(PRTable.class));
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type> {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager) {
            super(Type.class);
            this.typeManager = checkNotNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context) {
            Type type = typeManager.getType(parseTypeSignature(value));
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }
}
