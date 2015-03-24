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
package com.basho.riak.presto.cli;

import com.basho.riak.presto.models.PRSchema;
import com.basho.riak.presto.models.PRTable;
import com.basho.riak.presto.models.RiakColumnHandle;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableMap;
import javax.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.util.Locale.ENGLISH;

public final class Deserializer {
    public static final JsonCodec<Map<String, List<PRTable>>> CATALOG_CODEC;
    public static final JsonCodec<PRTable> TABLE_CODEC;
    public static final JsonCodec<RiakColumnHandle> COLUMN_CODEC;
    public static final JsonCodec<PRSchema> SCHEMA_CODEC;

    static {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.<Class<?>, JsonDeserializer<?>>of(Type.class, new CLITypeDeserializer()));
        JsonCodecFactory codecFactory = new JsonCodecFactory(objectMapperProvider);
        CATALOG_CODEC = codecFactory.mapJsonCodec(String.class, listJsonCodec(PRTable.class));
        TABLE_CODEC = codecFactory.jsonCodec(PRTable.class);
        SCHEMA_CODEC = codecFactory.jsonCodec(PRSchema.class);
        COLUMN_CODEC = codecFactory.jsonCodec(RiakColumnHandle.class);
    }

    private Deserializer() {
    }

    public static final class CLITypeDeserializer
            extends FromStringDeserializer<Type> {

        // @doc see com.facebook.presto.spi.types.StandardTypes
        // https://github.com/facebook/presto/blob/master/presto-spi/src/main/java/com/facebook/presto/spi/type/StandardTypes.java
        // for standard types
        private final Map<String, Type> types = ImmutableMap.<String, Type>of(
                StandardTypes.BOOLEAN, BOOLEAN,
                StandardTypes.BIGINT, BIGINT,
                StandardTypes.DOUBLE, DOUBLE,
                StandardTypes.VARCHAR, VARCHAR,
                StandardTypes.TIMESTAMP, TimestampType.TIMESTAMP
        );

        @Inject
        public CLITypeDeserializer() {
            super(Type.class);
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context) {
            Type type = types.get(value.toLowerCase(ENGLISH));
            if (type == null) {
                throw new IllegalArgumentException(String.valueOf("Unknown type " + value));
            }
            return type;
        }
    }
}
