package com.basho.riak.presto;

// /usr/local/erlang/R16B03-1/lib/erlang/lib/jinterface-1.5.8/priv/OtpErlang.jar
// or 1.5.6 on maven repo.
import com.ericsson.otp.erlang.*;

import java.io.IOException;
import java.util.Vector;

// @doc directly connect to Riak via distributed Erlang.
public class DirectConnection {
    private String peer;
    private String cookie;
    private OtpSelf self;
    private OtpPeer other;
    private OtpConnection conn;
    private OtpErlangObject local_client;
    public DirectConnection(String self, String cookie) throws java.io.IOException {
        this.self = new OtpSelf(self, cookie);
        this.cookie = cookie;
        //this.self = new OtpSelf(peer, cookie);
    }

    // there are no disconnect.
    public void connect(String other) throws java.io.IOException, OtpAuthException, OtpErlangExit {
        this.peer = other;
        this.other = new OtpPeer(other);
        this.conn = self.connect(this.other);
    }

    public void ping() throws java.io.IOException, OtpAuthException, OtpErlangExit {
        // connection test
        conn.sendRPC("erlang","date",new OtpErlangList());
        OtpErlangObject received = conn.receiveRPC();
        System.out.println(received);

        this.conn.sendRPC("riak","local_client",new OtpErlangList());
        OtpErlangTuple ok_or_error = (OtpErlangTuple)conn.receiveRPC();
        this.local_client = ok_or_error.elementAt(1);
        System.out.println(this.local_client);

    }

    public OtpConnection getConn(){
        return conn;
    }

    private <T> T call(String module, String function, OtpErlangList argv)
            throws IOException, OtpErlangExit, OtpAuthException
    {
        conn.sendRPC(module, function, argv);
        OtpErlangObject result = conn.receiveRPC();
        return (T)result;
    }

    public void get(byte[] b, byte[] k)
        throws java.io.IOException , OtpErlangExit , OtpAuthException {
        OtpErlangObject[] argv0 = {
            new OtpErlangBinary(b),
            new OtpErlangBinary(k),
            local_client};
        OtpErlangObject val = call("riak_client", "get", new OtpErlangList(argv0));
        System.out.println(val);
    }

    public OtpErlangObject vnodes() throws java.io.IOException , OtpErlangExit , OtpAuthException {
        conn.sendRPC("riak_core_ring_manager", "get_my_ring", new OtpErlangList());
        OtpErlangTuple ok_or_error = (OtpErlangTuple)conn.receiveRPC();
        OtpErlangObject ring = ok_or_error.elementAt(1);
        return ring;
    }

    public OtpErlangObject my_indices(OtpErlangObject ring)
        throws java.io.IOException , OtpErlangExit , OtpAuthException {
        OtpErlangObject[] argv = {ring};
        conn.sendRPC("riak_core_ring", "my_indices", new OtpErlangList(argv));
        OtpErlangObject indices = conn.receiveRPC();
        return indices;
    }

    public OtpErlangList getLocalCoverage(int reqid) throws java.io.IOException , OtpErlangExit , OtpAuthException {
        OtpErlangLong i = new OtpErlangLong(reqid);
        OtpErlangObject[] argv = {i};
        System.out.println(argv);
        conn.sendRPC("ldna", "get_local_coverage", new OtpErlangList(argv));
        OtpErlangObject cov = conn.receiveRPC();
        return (OtpErlangList)cov;
    }

    public OtpErlangList getSplits(int reqid) throws java.io.IOException , OtpErlangExit , OtpAuthException {
        OtpErlangObject[] argv = { new OtpErlangLong(reqid) };
        return this.call("ldna", "get_splits", new OtpErlangList(argv));
    }

    public OtpErlangTuple getCoveragePlan(int reqid)
        throws IOException, OtpErlangExit, OtpAuthException
    {
        OtpErlangObject[] argv = { new OtpErlangLong(reqid) };
        return this.call("ldna", "get_coverage_plan", new OtpErlangList(argv));
    }

    public OtpErlangList processSplits(byte[] bucket, OtpErlangTuple nodeSplits)
        throws java.io.IOException , OtpErlangExit , OtpAuthException
    {
        OtpErlangObject[] argv = {new OtpErlangBinary(bucket), nodeSplits};
        return this.call("ldna", "process_splits", new OtpErlangList(argv));
    }

    public OtpErlangList processSplit(byte[] bucket, OtpErlangTuple vnode,
                                      OtpErlangList filterVnodes)
            throws java.io.IOException , OtpErlangExit , OtpAuthException
    {
        OtpErlangObject[] argv = {new OtpErlangBinary(bucket), vnode, filterVnodes};
        return this.call("ldna", "process_split", new OtpErlangList(argv));
    }

    // vnode, bucket -> [riak_object()]
    public OtpErlangObject fetchVNodeData(OtpErlangObject vnode,
                                          OtpErlangBinary bucket) throws java.io.IOException , OtpErlangExit , OtpAuthException {
        OtpErlangObject[] argv = {vnode, bucket};
        conn.sendRPC("ldna", "fetch_vnode", argv);
        OtpErlangObject riakObjects = conn.receiveRPC();
        return riakObjects;
    }
}
