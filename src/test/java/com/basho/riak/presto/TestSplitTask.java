package com.basho.riak.presto;

import com.basho.riak.presto.SplitTask;
import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangTuple;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.codec.DecoderException;
import org.junit.Test;
import static org.junit.Assert.*;
/**
 * Created by kuenishi on 14/03/29.
 */
public class TestSplitTask {

    @Test
    public void testPasses() {
        String expected = "Hello, JUnit!";
        String hello = "Hello, JUnit!";
        assertEquals(hello, expected);
    }

    @Test
    public void testEncode()
            throws OtpErlangDecodeException, DecoderException
    {
        OtpErlangObject[] list = {new OtpErlangLong(12),
                new OtpErlangAtom("dev@127.0.0.1")};

        OtpErlangObject[] list2 = {new OtpErlangTuple(list),
                new OtpErlangAtom("dev@1.1.1.1")};
        OtpErlangTuple t = new OtpErlangTuple(list2);

        SplitTask splitTask = new SplitTask("dev@127.0.0.1", t);

        byte[] b = splitTask.term2binary(t);
        OtpErlangObject o = splitTask.binary2term(b);
        assertEquals(t, (OtpErlangTuple)o);

        String s = splitTask.toString();
        SplitTask task2 = new SplitTask(s);
        assertEquals(s, task2.toString());
        assertEquals(splitTask.getHost(), task2.getHost());
        assertEquals(splitTask.getTask(), task2.getTask());
        assertEquals(t, task2.getTask());
    }
}
