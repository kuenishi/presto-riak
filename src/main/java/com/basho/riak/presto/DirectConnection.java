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

// /usr/local/erlang/R16B03-1/lib/erlang/lib/jinterface-1.5.8/priv/OtpErlang.jar
// or 1.5.6 on maven repo.

import com.ericsson.otp.erlang.*;
import com.google.inject.Inject;

import java.io.IOException;

// @doc directly connect to Riak via distributed Erlang.
public class DirectConnection {
    private String peer;
    private String cookie;
    private OtpSelf self;
    private OtpPeer other;
    private OtpConnection conn;
    private OtpErlangObject local_client;

    @Inject
    public DirectConnection(RiakConfig riakConfig)
            throws IOException, OtpAuthException {
        this.cookie = riakConfig.getErlangCookie();
        this.self = new OtpSelf(riakConfig.getErlangNodeName(), cookie);
        this.peer = riakConfig.getLocalNode();

        this.other = new OtpPeer(this.peer);
        this.conn = self.connect(other);
    }

    public DirectConnection(String self, String cookie) throws java.io.IOException {
        this.self = new OtpSelf(self, cookie);
        this.cookie = cookie;
        //this.self = new OtpSelf(peer, cookie);
    }

    public void pid() {
        byte[] b = {'f', 'k'};
        OtpErlangBinary bin = new OtpErlangBinary(b);
        System.out.println(bin);
        System.out.println(bin.binaryValue().toString());
        System.out.println(new String(b));
        System.out.println(new String(bin.binaryValue()));
        System.exit(0);
    }

    // there are no disconnect.
    public void connect(String other) throws java.io.IOException, OtpAuthException, OtpErlangExit {
        this.peer = other;
        this.other = new OtpPeer(other);
        this.conn = self.connect(this.other);
    }

    public void ping() throws java.io.IOException, OtpAuthException, OtpErlangExit {
        // connection test

        OtpErlangObject received = call("erlang", "date", new OtpErlangList());
        System.out.println(received);


        OtpErlangTuple ok_or_error = (OtpErlangTuple) call("riak", "local_client", new OtpErlangList());
        this.local_client = ok_or_error.elementAt(1);
        System.out.println(this.local_client);

    }

    public OtpConnection getConn() {
        return conn;
    }

    private synchronized <T> T call(String module, String function, OtpErlangList argv)
            throws IOException, OtpErlangExit, OtpAuthException {
        conn.sendRPC(module, function, argv);
        OtpErlangObject result = conn.receiveRPC();
        return (T) result;
    }

    public void get(byte[] b, byte[] k)
            throws java.io.IOException, OtpErlangExit, OtpAuthException {
        OtpErlangObject[] argv0 = {
                new OtpErlangBinary(b),
                new OtpErlangBinary(k),
                local_client};
        OtpErlangObject val = call("riak_client", "get", new OtpErlangList(argv0));
        System.out.println(val);
    }

    public OtpErlangObject vnodes() throws java.io.IOException, OtpErlangExit, OtpAuthException {
        OtpErlangTuple ok_or_error = (OtpErlangTuple) call("riak_core_ring_manager", "get_my_ring", new OtpErlangList());
        OtpErlangObject ring = ok_or_error.elementAt(1);
        return ring;
    }

    public OtpErlangObject my_indices(OtpErlangObject ring)
            throws java.io.IOException, OtpErlangExit, OtpAuthException {
        OtpErlangObject[] argv = {ring};
        OtpErlangObject indices = call("riak_core_ring", "my_indices", new OtpErlangList(argv));
        return indices;
    }

    public OtpErlangList getLocalCoverage(int reqid) throws java.io.IOException, OtpErlangExit, OtpAuthException {
        OtpErlangLong i = new OtpErlangLong(reqid);
        OtpErlangObject[] argv = {i};
        System.out.println(argv);

        return (OtpErlangList) call("ldna", "get_local_coverage", new OtpErlangList(argv));
    }

    public OtpErlangList getSplits(int reqid) throws java.io.IOException, OtpErlangExit, OtpAuthException {
        OtpErlangObject[] argv = {new OtpErlangLong(reqid)};
        return this.call("ldna", "get_splits", new OtpErlangList(argv));
    }


    public OtpErlangTuple getCoveragePlan(int reqid)
            throws IOException, OtpErlangExit, OtpAuthException {
        OtpErlangObject[] argv = {new OtpErlangLong(reqid)};
        return this.call("ldna", "get_coverage_plan", new OtpErlangList(argv));
    }

    public OtpErlangList processSplits(byte[] bucketType, byte[] bucket, OtpErlangTuple nodeSplits)
            throws java.io.IOException, OtpErlangExit, OtpAuthException {
        OtpErlangObject[] bt = {new OtpErlangBinary(bucketType), new OtpErlangBinary(bucket)};
        OtpErlangObject[] argv = {new OtpErlangTuple(bt), nodeSplits};
        return this.call("ldna", "process_splits", new OtpErlangList(argv));
    }

    public OtpErlangList processSplit(byte[] bucketType, byte[] bucket, OtpErlangTuple vnode,
                                      OtpErlangList filterVnodes)
            throws java.io.IOException, OtpErlangExit, OtpAuthException {
        OtpErlangObject[] bt = {new OtpErlangBinary(bucketType), new OtpErlangBinary(bucket)};
        OtpErlangObject[] argv = {new OtpErlangTuple(bt), vnode, filterVnodes};
        return this.call("ldna", "process_split", new OtpErlangList(argv));
    }

    // index : in Riak it's foobar_int, foobar_bin but this is just a column name
    public OtpErlangTuple processSplitIndex(byte[] bucketType, byte[] bucket, OtpErlangTuple vnode,
                                            OtpErlangList filterVnodes,
                                            OtpErlangTuple query)
            throws java.io.IOException, OtpErlangExit, OtpAuthException {
        OtpErlangObject[] bt = {new OtpErlangBinary(bucketType), new OtpErlangBinary(bucket)};
        OtpErlangObject[] argv = {new OtpErlangTuple(bt), vnode, filterVnodes, query};
        return this.call("ldna", "process_split", new OtpErlangList(argv));
    }


    // vnode, bucket -> [riak_object()]
    public OtpErlangObject fetchVNodeData(OtpErlangObject vnode,
                                          OtpErlangBinary bucket) throws java.io.IOException, OtpErlangExit, OtpAuthException {
        OtpErlangObject[] argv = {vnode, bucket};
        return call("ldna", "fetch_vnode", new OtpErlangList(argv));
    }
}
