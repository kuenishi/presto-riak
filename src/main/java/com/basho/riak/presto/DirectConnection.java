package com.basho.riak.presto;

// /usr/local/erlang/R16B03-1/lib/erlang/lib/jinterface-1.5.8/priv/OtpErlang.jar
// or 1.5.6 on maven repo.
import com.ericsson.otp.erlang.*;
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

    public void get(byte[] b, byte[] k)
        throws java.io.IOException , OtpErlangExit , OtpAuthException {
              OtpErlangObject[] argv0 = {
            new OtpErlangBinary(b),
            new OtpErlangBinary(k),
            local_client};
        OtpErlangList argv1 = new OtpErlangList(argv0);
        System.out.println(argv1);
        conn.sendRPC("riak_client", "get", argv1);
        OtpErlangObject val = conn.receiveRPC();
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
        OtpErlangLong i = new OtpErlangLong(reqid);
        OtpErlangObject[] argv = {i};
        System.out.println(argv);
        conn.sendRPC("ldna", "get_splits", new OtpErlangList(argv));
        OtpErlangObject cov = conn.receiveRPC();
        return (OtpErlangList)cov;
    }

    public OtpErlangList processSplits(byte[] bucket, OtpErlangTuple nodeSplits)
        throws java.io.IOException , OtpErlangExit , OtpAuthException
    {
        OtpErlangObject[] argv = {new OtpErlangBinary(bucket), nodeSplits};
        System.out.println(argv);
        conn.sendRPC("ldna", "process_splits", new OtpErlangList(argv));
        OtpErlangObject objects = conn.receiveRPC();
        return (OtpErlangList)objects;
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
