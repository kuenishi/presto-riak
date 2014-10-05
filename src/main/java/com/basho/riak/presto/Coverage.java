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

import com.ericsson.otp.erlang.*;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;

public class Coverage {
    private final DirectConnection conn;
    private OtpErlangTuple coveragePlan;
    //private final node;

    public Coverage(DirectConnection conn) {
        //this.node = riakNode;
        this.conn = conn; //checkNotNull(conn);
        this.coveragePlan = null;
    }

    @NotNull
    public void plan() {
        try {
            //conn.ping();
            this.coveragePlan = conn.getCoveragePlan(9979);
        } catch (java.io.IOException e) {
            //  System.out.println(e);
        } catch (com.ericsson.otp.erlang.OtpAuthException e) {
            //  System.out.println(e);
        } catch (com.ericsson.otp.erlang.OtpErlangExit e) {
            //  System.out.println(e);
        }
    }

    public List<SplitTask> getSplits() {

        OtpErlangList vnodes = (OtpErlangList) this.coveragePlan.elementAt(0);
        OtpErlangObject filterVnodes = this.coveragePlan.elementAt(1);

        //System.out.println(vnodes);
        //System.out.println(filterVnodes);

        List<SplitTask> l = new ArrayList<SplitTask>();

        for (OtpErlangObject obj : vnodes) {

            // {Index, Node}
            OtpErlangTuple vnode = (OtpErlangTuple) obj;
            OtpErlangObject index = vnode.elementAt(0);
            String nodeName = ((OtpErlangAtom) vnode.elementAt(1)).atomValue();
            OtpErlangObject[] a = {vnode, filterVnodes};
            OtpErlangTuple t = new OtpErlangTuple(a);

            SplitTask task = new SplitTask(nodeName, t);
            l.add(task);
        }

        return l;
    }

    public String toString() {
        return coveragePlan.toString();
    }

    public void run()
            throws java.io.IOException, OtpErlangExit, OtpAuthException {
//        byte[] b = "foobartable".getBytes();
//        for(OtpErlangObject split : this.splits) {
//            OtpErlangTuple t = (OtpErlangTuple)split;
//            OtpErlangList r = conn.processSplits(b, t);
//            System.out.println(r);
//        }

    }
}