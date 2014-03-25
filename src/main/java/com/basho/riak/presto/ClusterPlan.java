package com.basho.riak.presto;

// /usr/local/erlang/R16B03-1/lib/erlang/lib/jinterface-1.5.8/priv/OtpErlang.jar
import com.ericsson.otp.erlang.*;  

import static com.google.common.base.Preconditions.checkNotNull;
import javax.validation.constraints.NotNull;

public class ClusterPlan {
    private final DirectConnection conn;
    //private final node;

    public ClusterPlan(DirectConnection conn){
        //this.node = riakNode;
        this.conn = conn; //checkNotNull(conn);
    }

    @NotNull
    public ClusterPlan build() {
        try{
            conn.connect("dev1@127.0.0.1");
        }
        catch( java.io.IOException e )
            {
            }
        catch( com.ericsson.otp.erlang.OtpAuthException e )
            {}
        catch( com.ericsson.otp.erlang.OtpErlangExit e )
            {}

        return this;
    }

    public String toString() {
        return "heh";
    }
}