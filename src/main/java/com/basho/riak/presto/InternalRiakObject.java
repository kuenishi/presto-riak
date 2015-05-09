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

import com.ericsson.otp.erlang.*;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;


/**
 * Created by kuenishi on 14/03/29.
 */
// @doc translated from internal riak_object.
// direct translation.
public class InternalRiakObject {
    private final byte[] key;
    private final byte[] bucket;
    private final byte[] bucketType;
    private final String vtag = "this will be vtag";
    private List<byte[]> values;

    public InternalRiakObject(OtpErlangObject object) {
        checkRecord(object, "r_object");

        OtpErlangTuple rObject = (OtpErlangTuple) object;
//        System.out.println(rObject);
//        -record(r_object, {
//                bucket :: bucket(),

        OtpErlangTuple bt = (OtpErlangTuple) rObject.elementAt(1);
        bucketType = ((OtpErlangBinary) bt.elementAt(0)).binaryValue();
        bucket = ((OtpErlangBinary) bt.elementAt(1)).binaryValue();
//                key :: key(),
        this.key = ((OtpErlangBinary) rObject.elementAt(2)).binaryValue();
//                contents :: [#r_content{}],
        OtpErlangList contents = (OtpErlangList) rObject.elementAt(3);
        values = new ArrayList();
        for (OtpErlangObject content : contents) {
            checkRecord(content, "r_content");
//            -record(r_content, {
//                    metadata :: dict() | list(),
//                    value :: term()
            OtpErlangBinary b = (OtpErlangBinary) ((OtpErlangTuple) content).elementAt(2);
            values.add(b.binaryValue());
//            }).
        }
//        vclock = vclock:fresh() :: vclock:vclock(),
//            updatemetadata=dict:store(clean, true, dict:new()) :: dict(),
//            updatevalue :: term()
//        }).
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getBucket() {
        return bucket;
    }

    public String getVTag() {
        return vtag;
    }

    private void checkRecord(OtpErlangObject o, String v) {
        checkAtom(((OtpErlangTuple) o).elementAt(0), v);
    }

    private void checkAtom(OtpErlangObject o, String v) {
        checkState(((OtpErlangAtom) o).atomValue().equals(v));
    }

    public void setValue(byte[] bytes) {
    }

    public byte[] getValue() {
//        if(values.size()==1)
        return values.get(0);
    }

    public void setValue(String s) {

    }

    public String getValueAsString() {
        return new String(getValue());
    }


}
