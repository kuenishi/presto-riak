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
        System.out.println(data);
        System.out.println(binary);
        task = (OtpErlangTuple)binary2term(binary);
        System.out.println(task);
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
            OtpErlangList riakObjects = conn.processSplitIndex(bucket.getBytes(), vnode,
                    filterVnodes, query);
            //System.out.println(riakObjects);
            return riakObjects;
        }
        catch (java.io.IOException e){
            System.err.println(e);
        }
        return new OtpErlangList();
    }

}
