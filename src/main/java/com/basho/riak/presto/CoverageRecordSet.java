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

import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.apache.commons.codec.DecoderException;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

//import com.google.common.io.InputSupplier;
//import com.google.common.io.Resources;

public class CoverageRecordSet
        implements RecordSet {
    private static final Logger log = Logger.get(CoverageRecordSet.class);
    private final String schemaName;
    private final String tableName;
    private final String bucketName;
    private final Optional<String> path;
    private final List<RiakColumnHandle> columnHandles;
    private final List<Type> types;
    private final List<HostAddress> addresses;
    //private final InputSupplier<InputStream> inputStreamSupplier;
    private final SplitTask splitTask;
    private final TupleDomain<ConnectorColumnHandle> tupleDomain;
    private final RiakConfig riakConfig;
    private final DirectConnection directConnection;


    public CoverageRecordSet(CoverageSplit split,
                             List<RiakColumnHandle> columnHandles,
                             RiakConfig riakConfig,
                             TupleDomain<ConnectorColumnHandle> tupleDomain,
                             DirectConnection directConnection)
            throws OtpErlangDecodeException, DecoderException {
        checkNotNull(split, "split is null");
        this.columnHandles = checkNotNull(columnHandles, "column handles is null");

        this.schemaName = checkNotNull(split.getSchemaName());
        this.tableName = checkNotNull(split.getTableName());
        this.bucketName = checkNotNull(split.getBucketName());
        this.path = split.getPath();

        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (RiakColumnHandle column : columnHandles) {
            types.add(column.getColumn().getType());
        }
        this.types = types.build();

        this.addresses = ImmutableList.copyOf(split.getAddresses());
        this.splitTask = checkNotNull(split.getSplitTask());
        this.tupleDomain = checkNotNull(tupleDomain);
        this.riakConfig = checkNotNull(riakConfig);
        this.directConnection = checkNotNull(directConnection);
    }

    @Override
    public List<Type> getColumnTypes() {
        return types;
    }

    @Override
    public RecordCursor cursor() {
        return new CoverageRecordCursor(schemaName, tableName, bucketName, path,
                columnHandles, addresses, splitTask, tupleDomain,
                riakConfig, directConnection);
    }
}
