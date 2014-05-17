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
import com.facebook.presto.spi.TupleDomain;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

/**
 * Created by kuenishi on 14/03/27.
 */
public class SplitTask {
    private final String host;
    private final OtpErlangTuple task; // OtpErlangObject[] a = {vnode, filterVnodes};


    public SplitTask(String node, OtpErlangTuple task)
    {
        this.host = node2host(node);
        this.task = task;
    }

    // fromString(String)
    public SplitTask(String data)
        throws OtpErlangDecodeException, DecoderException
    {

        byte[] binary = Base64.decodeBase64(Hex.decodeHex(data.toCharArray()));
        task = (OtpErlangTuple)binary2term(binary);
        // task = {vnode, filterVnodes}
        OtpErlangTuple vnode = (OtpErlangTuple)task.elementAt(0);
        // vnode = {index, node}
        OtpErlangAtom erlangNode = (OtpErlangAtom)vnode.elementAt(1);
        // "dev" @ "127.0.0.1"
        this.host = node2host(erlangNode.atomValue());
    }

    private String node2host(String node)
    {
        String[] s = node.split("@");
        return s[1];
    }

    // @doc hostname without port number: Riak doesn't care about port number.
    public String getHost()
    {
        return host;
    }

    public String toString()
    {
        byte[] binary = term2binary(task);
        byte[] b = Base64.encodeBase64(binary);
        return Hex.encodeHexString(b);
    }
    public byte[] term2binary(OtpErlangObject o)
    {
        OtpOutputStream oos = new OtpOutputStream();
        oos.write_any(o);
        return oos.toByteArray();
    }

    public OtpErlangObject binary2term(byte[] data)
            throws OtpErlangDecodeException
    {
        OtpInputStream ois = new OtpInputStream(data);
        return ois.read_any();
    }

    public OtpErlangTuple getTask(){ return this.task; }

    public OtpErlangList fetchAllData(DirectConnection conn, String schemaName, String tableName)
            throws OtpErlangDecodeException, OtpAuthException, OtpErlangExit
    {
        OtpErlangTuple t  = (OtpErlangTuple)task;
        OtpErlangTuple vnode = (OtpErlangTuple)t.elementAt(0);
        OtpErlangList filterVnodes = (OtpErlangList)t.elementAt(1);

        String bucket = schemaName + "." + tableName;
        try {
            OtpErlangList riakObjects = conn.processSplit(bucket.getBytes(), vnode, filterVnodes);
            //System.out.println(riakObjects);
            return riakObjects;
        }
        catch (java.io.IOException e){
            System.err.println(e);
        }
        return new OtpErlangList();
    }

    public OtpErlangList fetchViaIndex(DirectConnection conn, String schemaName, String tableName,
                                       OtpErlangTuple query)
            throws OtpErlangDecodeException, OtpAuthException, OtpErlangExit
    {
        OtpErlangTuple t  = (OtpErlangTuple)task;
        OtpErlangTuple vnode = (OtpErlangTuple)t.elementAt(0);
        OtpErlangList filterVnodes = (OtpErlangList)t.elementAt(1);

        String bucket = schemaName + "." + tableName;

        try {
            OtpErlangTuple result = conn.processSplitIndex(bucket.getBytes(), vnode,
                    filterVnodes, query);
            OtpErlangList riakObjects = (OtpErlangList)result.elementAt(1);
            //System.out.println(riakObjects);
            return riakObjects;
        }
        catch (java.io.IOException e){
            System.err.println(e);
        }
        return new OtpErlangList();
    }

}
