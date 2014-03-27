package com.basho.riak.presto;

import com.ericsson.otp.erlang.*;

/**
 * Created by kuenishi on 14/03/27.
 */
public class SplitTask {
    private final String node;
    private final OtpErlangObject task;

    public SplitTask(String node, OtpErlangObject task)
    {
        this.node = node;
        this.task = task;
    }

    public String getHost()
    {
        return node;
    }

    public void fetchAllData(DirectConnection conn, String schemaName, String tableName)
            throws OtpErlangDecodeException, OtpAuthException, OtpErlangExit
    {
//        OtpInputStream ois = new OtpInputStream(this.data);
        OtpErlangTuple t  = (OtpErlangTuple)task;
        OtpErlangTuple vnode = (OtpErlangTuple)t.elementAt(0);
        OtpErlangList filterVnodes = (OtpErlangList)t.elementAt(1);

        try {
//            DirectConnection conn = new DirectConnection("client2@127.0.0.1", "riak");
//            conn.connect("riak@127.0.0.1");
            //conn.ping();
            OtpErlangList riakObjects = conn.processSplit(tableName.getBytes(), vnode, filterVnodes);
            System.out.println("============printing split data===============");
            System.out.println(riakObjects);
        }
        catch (java.io.IOException e){
            System.err.println(e);
        }
    }
}
