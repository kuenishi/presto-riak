package com.basho.riak.presto;

// /usr/local/erlang/R16B03-1/lib/erlang/lib/jinterface-1.5.8/priv/OtpErlang.jar
import com.ericsson.otp.erlang.*;

import static com.google.common.base.Preconditions.checkNotNull;
import javax.validation.constraints.NotNull;

public class ClusterPlan {
    private final DirectConnection conn;
    private OtpErlangList splits;
    //private final node;

    public ClusterPlan(DirectConnection conn){
        //this.node = riakNode;
        this.conn = conn; //checkNotNull(conn);
        this.splits = null;
    }

    @NotNull
    public ClusterPlan build() {
        try{
            //conn.ping();

            this.splits = conn.getSplits(9979);
        }
        catch( java.io.IOException e ) {
            System.out.println(e);
        }
        catch( com.ericsson.otp.erlang.OtpAuthException e ) {
            System.out.println(e);
        }
        catch( com.ericsson.otp.erlang.OtpErlangExit e ) {
            System.out.println(e);
        }

        return this;
    }

    public String toString() {
        return splits.toString();
    }

    public void run()
        throws java.io.IOException, OtpErlangExit, OtpAuthException
    {
        byte[] b = "foobartable".getBytes();
        for(OtpErlangObject split : this.splits) {
            OtpErlangTuple t = (OtpErlangTuple)split;
            OtpErlangList r = conn.processSplits(b, t);
            System.out.println(r);
        }

    }
}