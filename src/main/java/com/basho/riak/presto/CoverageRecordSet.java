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

import com.basho.riak.presto.models.CoverageSplit;
import com.basho.riak.presto.models.RiakColumnHandle;
import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.apache.commons.codec.DecoderException;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

//import com.google.common.io.InputSupplier;
//import com.google.common.io.Resources;

public class CoverageRecordSet
        implements RecordSet {
    private static final Logger log = Logger.get(CoverageRecordSet.class);
    private final CoverageSplit split;
    private final List<RiakColumnHandle> columnHandles;
    private final List<Type> types;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final DirectConnection directConnection;


    public CoverageRecordSet(CoverageSplit split,
                             List<RiakColumnHandle> columnHandles,
                             RiakConfig riakConfig,
                             TupleDomain<ColumnHandle> tupleDomain,
                             DirectConnection directConnection) {
        this.split = checkNotNull(split, "split is null");
        this.columnHandles = checkNotNull(columnHandles, "column handles is null");


        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (RiakColumnHandle column : columnHandles) {
            types.add(column.getColumn().getType());
        }
        this.types = types.build();
        this.tupleDomain = checkNotNull(tupleDomain);
        this.directConnection = checkNotNull(directConnection);
    }

    @Override
    public List<Type> getColumnTypes() {
        return types;
    }

    @Override
    public RecordCursor cursor() {
        try {
            return new CoverageRecordCursor(
                    split,
                    columnHandles, tupleDomain,
                    directConnection);
        } catch (OtpErlangDecodeException e) {
            log.error(e.getMessage());
        } catch (DecoderException e) {
            log.error(e.getMessage());
        }
        return null;
    }
}
